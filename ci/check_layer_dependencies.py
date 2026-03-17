"""
check_layer_dependencies.py

Parses a dbt manifest.json and enforces the layer dependency rule:
    staging  -> can only ref: (nothing above it)
    core     -> can only ref: staging
    intermediate -> can only ref: staging, core
    marts    -> can only ref: staging, core, intermediate
    reporting -> can only ref: staging, core, intermediate, marts

Any violation (e.g. core referencing intermediate, or intermediate referencing marts)
is printed and causes a non-zero exit code, failing the CI job.
"""

import json
import sys
from pathlib import Path

# Layer order — lower index = lower in the stack
LAYERS = ["staging", "core", "intermediate", "marts", "reporting"]

# Allowed upstream layers for each layer (can only ref layers below itself)
ALLOWED_UPSTREAM = {
    "staging":      set(),
    "core":         {"staging"},
    "intermediate": {"staging", "core"},
    "marts":        {"staging", "core", "intermediate"},
    "reporting":    {"staging", "core", "intermediate", "marts"},
}


def get_layer(node: dict) -> str | None:
    """Derive the layer from the node's fqn path."""
    fqn = node.get("fqn", [])
    # fqn looks like: ["heymax_case", "core", "fct_events"]
    # the layer is the second element (index 1) for project models
    if len(fqn) >= 2:
        candidate = fqn[1]
        if candidate in LAYERS:
            return candidate
    return None


def check_manifest(manifest_path: str) -> list[str]:
    violations = []

    with open(manifest_path) as f:
        manifest = json.load(f)

    nodes = manifest.get("nodes", {})

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue

        node_layer = get_layer(node)
        if node_layer is None:
            continue

        allowed = ALLOWED_UPSTREAM.get(node_layer, set())
        model_name = node.get("name", node_id)

        for dep_id in node.get("depends_on", {}).get("nodes", []):
            dep_node = nodes.get(dep_id)
            if dep_node is None:
                continue
            if dep_node.get("resource_type") != "model":
                continue

            dep_layer = get_layer(dep_node)
            if dep_layer is None:
                continue

            if dep_layer not in allowed and dep_layer != node_layer:
                dep_name = dep_node.get("name", dep_id)
                violations.append(
                    f"  VIOLATION: [{node_layer}] {model_name} "
                    f"references [{dep_layer}] {dep_name} "
                    f"(allowed upstream layers: {sorted(allowed) or 'none'})"
                )

    return violations


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_layer_dependencies.py <path/to/manifest.json>")
        sys.exit(1)

    manifest_path = sys.argv[1]

    if not Path(manifest_path).exists():
        print(f"ERROR: manifest not found at {manifest_path}")
        sys.exit(1)

    print(f"Checking layer dependencies in: {manifest_path}\n")
    violations = check_manifest(manifest_path)

    if violations:
        print(f"Found {len(violations)} layer dependency violation(s):\n")
        for v in violations:
            print(v)
        print("\nRule: core > intermediate > marts > reporting")
        print("Each layer may only reference models from layers below it.")
        sys.exit(1)
    else:
        print("All layer dependencies are valid.")
        sys.exit(0)
