import yaml
import os
import subprocess
import threading
import tkinter as tk
from datetime import datetime


def load_yaml(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML: {e}")


def is_service_running(service):
    stdout = subprocess.getoutput("docker ps --format '{{.Names}}'")
    running_services = stdout.split("\n")
    return any(service in name for name in running_services)


def write_log_line(service, line, log_file):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def start_service(service, button):
    def task():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/{service}_{timestamp}.log"

        print(f"[INFO] Lancement du service : {service}")
        print(f"[LOG] Fichier : {log_file}")

        # Lance docker-compose en mode non détaché
        process = subprocess.Popen(
            ["docker-compose", "up", service],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        # Lecture des logs en streaming
        for line in process.stdout:
            line = line.rstrip()
            print(f"[{service}] {line}")
            write_log_line(service, line, log_file)

        process.wait()
        print(f"[INFO] Service {service} terminé avec code {process.returncode}")

    threading.Thread(target=task, daemon=True).start()

def start_all_services(services, buttons):
    print("[INFO] Lancement de TOUS les services individuellement")
    if "orchestrator" in services:
        services.remove("orchestrator")
    print(services)
    for service, btn in zip(services, buttons):
        start_service(service, btn)

def update_button_colors(services, buttons, root):
    for service, btn in zip(services, buttons):
        if is_service_running(service):
            btn.config(bg="green")
        else:
            btn.config(bg="red")

    root.after(2000, lambda: update_button_colors(services, buttons, root))


def build_gui(services):
    root = tk.Tk()
    root.title("Docker Compose Launcher")

    tk.Label(root, text="Services Docker", font=("Arial", 16)).pack(pady=10)

    frame = tk.Frame(root)
    frame.pack()

    buttons = []

    # --- 1) Button for orchestrator (if exists) ---
    if "orchestrator" in services:
        orchestrator_btn = tk.Button(
            root,
            text="orchestrator",
            width=30,
            bg="orange",
        )
        orchestrator_btn.pack(pady=5)
        orchestrator_btn.config(command=lambda: start_service("orchestrator", orchestrator_btn))
        buttons.append(orchestrator_btn)

    # --- 2) Buttons for all other services ---
    service_buttons = []
    for service in services:
        if service == "orchestrator":
            continue

        btn = tk.Button(
            frame,
            text=service,
            width=25,
            bg="light grey",
        )
        btn.pack(pady=5)
        btn.config(command=lambda s=service, b=btn: start_service(s, b))

        service_buttons.append(btn)
        buttons.append(btn)

    # --- 3) Single "Start All" button (without orchestrator) ---
    all_btn = tk.Button(
        root,
        text="Lancer TOUS les scrapper",
        width=30,
        bg="blue",
        fg="white",
    )
    all_btn.pack(pady=15)
    all_btn.config(command=lambda: start_all_services(
        [s for s in services if s != "orchestrator"],
        service_buttons
    ))

    buttons.append(all_btn)

    # --- 4) Update colors ---
    update_button_colors(services, buttons, root)

    root.mainloop()



if __name__ == "__main__":
    yaml_file = "docker-compose.yml"

    try:
        config = load_yaml(yaml_file)
    except Exception as err:
        print(f"Erreur: {err}")
        exit(1)

    services_list = list(config["services"].keys())

    print("Services détectés :", services_list)

    build_gui(services_list)
