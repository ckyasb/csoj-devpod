"use client";

import { Suspense } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import useSWR from "swr";
import { format } from "date-fns";
import {
  Cpu,
  HardDrive,
  MemoryStick,
  Play,
  Plus,
  Square,
  Terminal,
  Trash2,
} from "lucide-react";

import api from "@/lib/api";
import { DevPodSession } from "@/lib/types";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { CopyButton } from "@/components/ui/shadcn-io/copy-button";
import { useToast } from "@/hooks/use-toast";
import { DevPodStatusBadge } from "@/components/devpods/devpod-status-badge";
import { DevPodDetails } from "@/components/devpods/devpod-details";

const fetcher = (url: string) => api.get(url).then((res) => res.data.data);

function DevPodsPageContent() {
  const searchParams = useSearchParams();
  const selectedID = searchParams.get("id");
  const { toast } = useToast();
  const { data: devpods, isLoading, mutate } = useSWR<DevPodSession[]>(
    "/devpods",
    fetcher,
    { refreshInterval: 5000 }
  );

  const action = async (id: string, verb: "start" | "stop" | "delete") => {
    try {
      if (verb === "delete") {
        if (!confirm("Delete this DevPod?")) return;
        await api.delete(`/devpods/${id}`);
      } else {
        await api.post(`/devpods/${id}/${verb}`);
      }
      toast({ title: `DevPod ${verb} requested` });
      mutate();
    } catch (error: any) {
      toast({
        variant: "destructive",
        title: `Failed to ${verb} DevPod`,
        description: error.response?.data?.message || "The request failed.",
      });
    }
  };

  if (isLoading) {
    return <Skeleton className="h-96 w-full" />;
  }

  if (selectedID) {
    return <DevPodDetails id={selectedID} />;
  }

  return (
    <Card>
      <CardHeader className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div>
          <CardTitle className="flex items-center gap-2 text-2xl">
            <Terminal className="h-6 w-6" />
            DevPods
          </CardTitle>
          <CardDescription>
            Interactive Kubernetes-backed development containers.
          </CardDescription>
        </div>
        <Button asChild>
          <Link href="/devpods/new">
            <Plus className="h-4 w-4" />
            New DevPod
          </Link>
        </Button>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Resources</TableHead>
              <TableHead>Network</TableHead>
              <TableHead>SSH</TableHead>
              <TableHead>Expires</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {devpods && devpods.length > 0 ? (
              devpods.map((pod) => (
                <TableRow key={pod.id}>
                  <TableCell>
                    <Link
                      href={`/devpods?id=${pod.id}`}
                      className="font-medium text-primary hover:underline"
                    >
                      {pod.displayName || pod.name}
                    </Link>
                    <div className="font-mono text-xs text-muted-foreground">
                      {pod.name}
                    </div>
                  </TableCell>
                  <TableCell>
                    <DevPodStatusBadge status={pod.status} />
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                      <span className="flex items-center gap-1">
                        <Cpu className="h-3.5 w-3.5" />
                        {pod.cpu}
                      </span>
                      <span className="flex items-center gap-1">
                        <MemoryStick className="h-3.5 w-3.5" />
                        {pod.memoryMB} MB
                      </span>
                      <span className="flex items-center gap-1">
                        <HardDrive className="h-3.5 w-3.5" />
                        {pod.persistent ? `${pod.storageGB} GB` : "ephemeral"}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <div>{pod.networkProfile}</div>
                    <div className="text-xs text-muted-foreground">
                      {pod.mpiEnabled ? "MPI" : "single pod"}
                      {pod.hostNetwork ? " · hostNetwork" : ""}
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex max-w-[320px] items-center gap-2">
                      <code className="truncate rounded bg-muted px-2 py-1 text-xs">
                        {pod.sshCommand}
                      </code>
                      <CopyButton content={pod.sshCommand} size="sm" />
                    </div>
                  </TableCell>
                  <TableCell>
                    {pod.expiresAt
                      ? format(new Date(pod.expiresAt), "MM/dd HH:mm")
                      : "-"}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      {pod.status === "Stopped" ? (
                        <Button
                          size="icon"
                          variant="outline"
                          onClick={() => action(pod.id, "start")}
                        >
                          <Play className="h-4 w-4" />
                        </Button>
                      ) : (
                        <Button
                          size="icon"
                          variant="outline"
                          disabled={
                            pod.status === "Deleted" ||
                            pod.status === "Deleting" ||
                            pod.status === "Expired"
                          }
                          onClick={() => action(pod.id, "stop")}
                        >
                          <Square className="h-4 w-4" />
                        </Button>
                      )}
                      <Button
                        size="icon"
                        variant="destructive"
                        disabled={pod.status === "Deleted"}
                        onClick={() => action(pod.id, "delete")}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={7} className="text-center text-muted-foreground">
                  No DevPods yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

export default function DevPodsPage() {
  return (
    <Suspense fallback={<Skeleton className="h-96 w-full" />}>
      <DevPodsPageContent />
    </Suspense>
  );
}
