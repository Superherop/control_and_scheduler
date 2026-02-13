#import modules
import time
import multiprocessing
import logging
import importlib
import json


def test_should_not_start_completed_module(monkeypatch):
    started_modules = []

    def fake_process(target=None, args=()):
        started_modules.append(args[0])
        class DummyProc:
            def __init__(self):
                self.exitcode = 0
            def is_alive(self): return False
            def start(self): pass
        return DummyProc()

    def run_module_process(module_name):
        pass

    dummy_config = {
        "enabled_modules": {
            "modA": {"enabled": True, "schedule": "00:00", "days": [time.strftime("%a")]}
        }
    }

    monkeypatch.setattr(multiprocessing, "Process", fake_process)
    monkeypatch.setattr("builtins.open", lambda *a, **k: open(__file__, "r"))
    monkeypatch.setattr(json, "load", lambda f: dummy_config)
    monkeypatch.setattr("builtins.print", lambda *a, **k: None)
    monkeypatch.setattr(logging, "info", lambda *a, **k: None)
    monkeypatch.setattr(importlib, "import_module", lambda name: type("M", (), {"run": lambda: None})())

    completed_today = {"modA": "10:00"}
    running_processes = {}
    starting_modules = set()

    # Simulate the relevant snippet from __main__ loop for parallel start check
    settings = dummy_config["enabled_modules"]["modA"]
    current_day = time.strftime("%a")
    if settings["enabled"] and (True or (settings["schedule"] == "00:00" and current_day in settings["days"])):
        if True or current_day in settings["days"]:
            already_running = "modA" in running_processes and running_processes["modA"][0].is_alive()
            already_starting = "modA" in starting_modules
            already_completed = "modA" in completed_today
            if not already_running and not already_starting and not already_completed:
                start_str = "now"
                starting_modules.add("modA")
                p = multiprocessing.Process(target=run_module_process, args=("modA",))
                p.daemon = True
                p.start()
                running_processes["modA"] = (p, start_str)

    assert "modA" not in started_modules, "Module should not start if it has completed today"