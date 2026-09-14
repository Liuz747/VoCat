/**
 * Browser side of the call PCM bridge.
 *
 * The server speaks little-endian signed 16-bit mono PCM at 8 kHz over a binary
 * WebSocket. The microphone is captured through Web Audio, resampled to 8 kHz
 * and sent in ~64 ms frames; downlink frames are resampled back to the
 * context rate and scheduled on a small jitter buffer.
 */

export const BRIDGE_RATE = 8000;

export type BridgeStatus = "idle" | "ready" | "connecting" | "connected" | "closed" | "error";

export interface BridgeEvents {
  onStatus?: (status: BridgeStatus, detail?: string) => void;
}

/** Linear-interpolation resampler; good enough for narrowband speech. */
export function resample(input: Float32Array, fromRate: number, toRate: number): Float32Array {
  if (fromRate === toRate || input.length === 0) return input;
  const ratio = fromRate / toRate;
  const length = Math.max(1, Math.round(input.length / ratio));
  const out = new Float32Array(length);
  for (let i = 0; i < length; i++) {
    const position = i * ratio;
    const index = Math.floor(position);
    const next = Math.min(index + 1, input.length - 1);
    const fraction = position - index;
    out[i] = input[index] * (1 - fraction) + input[next] * fraction;
  }
  return out;
}

export function floatToInt16(input: Float32Array): Int16Array {
  const out = new Int16Array(input.length);
  for (let i = 0; i < input.length; i++) {
    const sample = Math.max(-1, Math.min(1, input[i]));
    out[i] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
  }
  return out;
}

export function int16ToFloat(input: Int16Array): Float32Array {
  const out = new Float32Array(input.length);
  for (let i = 0; i < input.length; i++) out[i] = input[i] / 0x8000;
  return out;
}

export class CallAudioBridge {
  private context: AudioContext | null = null;
  private stream: MediaStream | null = null;
  private source: MediaStreamAudioSourceNode | null = null;
  private processor: ScriptProcessorNode | null = null;
  private sink: GainNode | null = null;
  private socket: WebSocket | null = null;
  private playhead = 0;
  private status: BridgeStatus = "idle";
  private readonly events: BridgeEvents;

  constructor(events: BridgeEvents = {}) {
    this.events = events;
  }

  get state(): BridgeStatus {
    return this.status;
  }

  private setStatus(status: BridgeStatus, detail?: string) {
    this.status = status;
    this.events.onStatus?.(status, detail);
  }

  /**
   * Acquire the microphone and an AudioContext. Must run inside a user
   * gesture (the dial/answer click) or browsers keep the context suspended.
   */
  async prepare(): Promise<void> {
    if (this.context && this.stream) return;
    if (!navigator.mediaDevices?.getUserMedia) {
      throw new Error("getUserMedia unavailable");
    }
    let context: AudioContext;
    try {
      context = new AudioContext({ sampleRate: BRIDGE_RATE });
    } catch {
      context = new AudioContext();
    }
    if (context.state === "suspended") {
      await context.resume().catch(() => {});
    }
    const stream = await navigator.mediaDevices.getUserMedia({
      audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true },
      video: false,
    });
    this.context = context;
    this.stream = stream;
    this.setStatus("ready");
  }

  /** Open the PCM socket for one call. prepare() must have succeeded. */
  connect(url: string): void {
    const context = this.context;
    const stream = this.stream;
    if (!context || !stream) throw new Error("bridge not prepared");
    this.disconnectSocket();
    this.setStatus("connecting");
    const socket = new WebSocket(url);
    socket.binaryType = "arraybuffer";
    this.socket = socket;
    this.playhead = 0;

    socket.onopen = () => {
      if (this.socket !== socket) return;
      this.startUplink(socket);
      this.setStatus("connected");
    };
    socket.onmessage = (event) => {
      if (this.socket !== socket || !(event.data instanceof ArrayBuffer)) return;
      this.playDownlink(new Int16Array(event.data));
    };
    socket.onerror = () => {
      if (this.socket !== socket) return;
      this.setStatus("error", "socket");
    };
    socket.onclose = (event) => {
      if (this.socket !== socket) return;
      this.stopUplink();
      this.socket = null;
      this.setStatus(this.status === "error" ? "error" : "closed", event.reason || String(event.code));
    };
  }

  private startUplink(socket: WebSocket) {
    const context = this.context;
    const stream = this.stream;
    if (!context || !stream) return;
    const source = context.createMediaStreamSource(stream);
    // 512 frames at 8 kHz is 64 ms; a ScriptProcessor must stay connected to
    // the destination to run, so it feeds a muted gain node.
    const processor = context.createScriptProcessor(512, 1, 1);
    const sink = context.createGain();
    sink.gain.value = 0;
    processor.onaudioprocess = (event) => {
      if (socket.readyState !== WebSocket.OPEN) return;
      const input = event.inputBuffer.getChannelData(0);
      const narrow = resample(input, context.sampleRate, BRIDGE_RATE);
      const pcm = floatToInt16(narrow);
      socket.send(pcm.buffer);
    };
    source.connect(processor);
    processor.connect(sink);
    sink.connect(context.destination);
    this.source = source;
    this.processor = processor;
    this.sink = sink;
  }

  private stopUplink() {
    try {
      this.processor?.disconnect();
      this.source?.disconnect();
      this.sink?.disconnect();
    } catch {
      /* already torn down */
    }
    if (this.processor) this.processor.onaudioprocess = null;
    this.processor = null;
    this.source = null;
    this.sink = null;
  }

  private playDownlink(pcm: Int16Array) {
    const context = this.context;
    if (!context || pcm.length === 0) return;
    const wide = resample(int16ToFloat(pcm), BRIDGE_RATE, context.sampleRate);
    const buffer = context.createBuffer(1, wide.length, context.sampleRate);
    buffer.copyToChannel(wide, 0);
    const node = context.createBufferSource();
    node.buffer = buffer;
    node.connect(context.destination);
    // Keep ~60 ms of jitter buffer; if playback fell behind, restart just ahead of now.
    const now = context.currentTime;
    if (this.playhead < now + 0.02) this.playhead = now + 0.06;
    node.start(this.playhead);
    this.playhead += buffer.duration;
  }

  private disconnectSocket() {
    const socket = this.socket;
    this.socket = null;
    this.stopUplink();
    if (socket) {
      socket.onopen = null;
      socket.onmessage = null;
      socket.onerror = null;
      socket.onclose = null;
      try {
        socket.close(1000, "call ended");
      } catch {
        /* ignore */
      }
    }
  }

  /** Close the socket but keep the microphone for the next call. */
  hangup(): void {
    this.disconnectSocket();
    if (this.context && this.stream) this.setStatus("ready");
    else this.setStatus("idle");
  }

  /** Release everything, including the microphone. */
  stop(): void {
    this.disconnectSocket();
    this.stream?.getTracks().forEach((track) => track.stop());
    this.stream = null;
    const context = this.context;
    this.context = null;
    if (context) context.close().catch(() => {});
    this.setStatus("idle");
  }
}
