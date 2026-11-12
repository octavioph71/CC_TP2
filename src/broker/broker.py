"""Broker — esboço: espera duration ou stop."""
def run(node_id, cfg, stop):
    print(f"[{node_id}] broker em {cfg.bind}:{cfg.control_port}/{cfg.data_port}", flush=True)
    stopped = stop.wait(cfg.duration)
    print(f"[{node_id}] fim ({'sinal' if stopped else 'duration'})", flush=True)
    return 0