"use client";

import React, { useCallback } from "react";
import { ReactFlow, 
  Background,
  Controls,
  MiniMap,
  MarkerType,
  useEdgesState,
  useNodesState,
  Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

const nodeStyle = {
  borderRadius: 12,
  border: "1px solid #93c5fd",
  padding: 8,
  background: "linear-gradient(180deg, #f0f9ff, #ffffff)",
  fontSize: 12,
  color: "#0f172a",
  minWidth: 160,
  textAlign: "center" as const,
  whiteSpace: "pre-line" as const,
  boxShadow: "0 4px 12px rgba(14,165,233,0.12)",
};

const initialNodes = [
  { id: "api", position: { x: 0, y: 0 }, data: { label: "🌐 Polygon API" }, style: nodeStyle, sourcePosition: Position.Right, type: "input" },
  { id: "producer", position: { x: 250, y: 0 }, data: { label: "⚙️ Producer" }, style: nodeStyle, sourcePosition: Position.Right, targetPosition: Position.Left },
  { id: "kafka", position: { x: 520, y: 0 }, data: { label: "🧩 Kafka\nstock_prices" }, style: nodeStyle, sourcePosition: Position.Right, targetPosition: Position.Left },
  { id: "db", position: { x: 800, y: 120 }, data: { label: "🗄️ DB consumer" }, style: nodeStyle, sourcePosition: Position.Right, targetPosition: Position.Left },
  { id: "pg", position: { x: 1080, y: 120 }, data: { label: "🐘 Postgres" }, style: nodeStyle, targetPosition: Position.Left, type: "output" },
  { id: "alerts", position: { x: 800, y: 220 }, data: { label: "🔔 Alerts consumer" }, style: nodeStyle, targetPosition: Position.Left },
];

const initialEdges = [
  edge("api", "producer", "REST JSON"),
  edge("producer", "kafka", "publish"),
  edge("kafka", "db", "consume", true),
  edge("db", "pg", "insert"),
  edge("kafka", "alerts", "analyze", true),
];

function edge(source: string, target: string, label?: string, animated = false) {
  return {
    id: `${source}-${target}`,
    source,
    target,
    label,
    labelBgPadding: [6, 2],
    labelBgBorderRadius: 6,
    animated,
    markerEnd: { type: MarkerType.ArrowClosed, color: "#22c55e" },
    style: { stroke: "#22c55e", strokeWidth: 2 },
  } as const;
}

export default function FlowInteractive() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const onConnect = useCallback((params: any) => setEdges((eds) => [...eds, { ...params, markerEnd: { type: MarkerType.ArrowClosed } }]), [setEdges]);

  return (
    <div className="w-full h-[360px] rounded-xl border border-gray-200 dark:border-neutral-800 overflow-hidden">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        fitView
      >
        <MiniMap
          pannable
          zoomable
          style={{ width: 120, height: 80, bottom: 8, right: 8, backgroundColor: "rgba(0,0,0,0.3)", borderRadius: 8 }}
        />
        <Controls position="bottom-right" />
        <Background gap={16} color="#e2e8f0" />
      </ReactFlow>
    </div>
  );
}


