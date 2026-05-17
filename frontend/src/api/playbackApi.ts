const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "/api";

export type QueuePlaylistSyncResponse = {
  playlist_id: string | null;
  playlist_uri: string | null;
  playlist_url: string | null;
  name: string;
  item_count: number;
};

export async function syncQueuePlaylist(uris: string[]): Promise<QueuePlaylistSyncResponse> {
  const response = await fetch(`${apiBaseUrl}/auth/playback/queue-playlist/sync`, {
    method: "POST",
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ uris }),
  });
  if (!response.ok) {
    let detail = `Queue playlist sync failed (${response.status}).`;
    try {
      const payload = (await response.json()) as { detail?: string };
      if (payload.detail) {
        detail = payload.detail;
      }
    } catch {
      // Keep fallback detail.
    }
    throw new Error(detail);
  }
  return (await response.json()) as QueuePlaylistSyncResponse;
}
