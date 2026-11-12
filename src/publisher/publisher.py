"""Publicador — esboço: espera start_after + drain ou stop."""
def run(node_id, cfg, stop):
    print(f"[{node_id}] publicador -> {cfg.broker_control}", flush=True)
    stop.wait(cfg.start_after + cfg.drain)
    print(f"[{node_id}] fim", flush=True)
    return 0