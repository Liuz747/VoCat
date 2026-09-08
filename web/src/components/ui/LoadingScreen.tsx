export function LoadingScreen() {
  return (
    <div className="app-boot-loader" role="status" aria-live="polite" aria-label="Loading">
      <span className="h-8 w-8 animate-spin rounded-full border-2 border-sky-500/20 border-t-sky-500 motion-reduce:animate-none" aria-hidden="true" />
      <span className="sr-only">Loading</span>
    </div>
  );
}
