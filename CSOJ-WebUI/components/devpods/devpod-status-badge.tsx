import { Badge } from "@/components/ui/badge";
import { DevPodStatus } from "@/lib/types";

export function DevPodStatusBadge({ status }: { status: DevPodStatus }) {
  const variant =
    status === "Running"
      ? "default"
      : status === "Failed" || status === "Deleted" || status === "Expired"
        ? "destructive"
        : status === "Stopped"
          ? "outline"
          : "secondary";

  return <Badge variant={variant}>{status}</Badge>;
}
