"""Subscritor — esboço: espera duration ou stop."""
def run(node_id, cfg, stop):
    print(f"[{node_id}] subscritor -> {cfg.broker_control}", flush=True)
    stop.wait(cfg.duration)
    print(f"[{node_id}] fim", flush=True)
    return 0
