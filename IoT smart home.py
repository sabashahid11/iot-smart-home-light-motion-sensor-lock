import threading
import time
import collections
from datetime import datetime

# =====================================================
# IOT SMART HOME: LIGHT AND MOTION SENSOR LOCK
# OS LAB PROJECT
#
# Concepts Applied:
# - Multithreading (concurrent users)
# - Binary Semaphores (device resource guards)
# - Mutex Lock (critical section protection)
# - FIFO Queue (starvation-free allocation)
# - Central Allocator Thread (fair dispatch)
# - Atomic Acquisition (deadlock prevention)
# =====================================================


# ------------------------------------------------
# DEVICE TIMING CONSTANTS (fixed for analysis)
# ------------------------------------------------
LIGHT_USE_TIME   = 4   # seconds light stays ON per user
CAMERA_REC_TIME  = 3   # seconds camera records per user
DOOR_HOLD_TIME   = 3   # seconds door stays unlocked


# ------------------------------------------------
# BINARY SEMAPHORES (1 = available, 0 = in use)
# ------------------------------------------------
light_sem  = threading.Semaphore(1)
camera_sem = threading.Semaphore(1)
door_sem   = threading.Semaphore(1)

# ------------------------------------------------
# FIFO QUEUES (one per device)
# Each entry: (user_name, threading.Event)
# Allocator pops from front; users enqueue at back
# ------------------------------------------------
light_queue  = collections.deque()
camera_queue = collections.deque()
door_queue   = collections.deque()

# Condition variables tied to a single queue_lock
# Allocator sleeps on these; release_* signals them
queue_lock        = threading.Lock()
light_condition   = threading.Condition(queue_lock)
camera_condition  = threading.Condition(queue_lock)
door_condition    = threading.Condition(queue_lock)

# ------------------------------------------------
# SHARED STATE (protected by system_lock)
# ------------------------------------------------
system_lock   = threading.Lock()
print_lock    = threading.Lock()

light_status  = "OFF"
door_status   = "LOCKED"
camera_status = "OFF"

logs          = []
active_users  = []
served_users  = []
threads       = []

total_served   = 0
granted_count  = 0
denied_count   = 0

# ------------------------------------------------
# TIME TRACKING (for analysis)
# ------------------------------------------------
system_start_time = None
completion_times  = []   # (user, elapsed_seconds)


# ===================================================
# UTILITIES
# ===================================================

def current_time():
    return datetime.now().strftime("%H:%M:%S")

def elapsed():
    if system_start_time is None:
        return 0.0
    return round(time.time() - system_start_time, 1)

def line(char="=", n=62):
    print(char * n)

def add_log(message):
    with system_lock:
        logs.append(f"[{current_time()}] {message}")

def tprint(message):
    """Thread-safe print."""
    with print_lock:
        print(message)


# ===================================================
# FIFO ALLOCATORS (one background thread each)
# ===================================================

def _run_allocator(queue, condition, semaphore, device_name):
    """Generic FIFO allocator for a single device."""
    while True:
        with condition:
            while len(queue) == 0:
                condition.wait()
            user_name, event = queue[0]
            semaphore.acquire()
            queue.popleft()
            event.set()

def light_allocator():
    _run_allocator(light_queue, light_condition, light_sem, "Light")

def camera_allocator():
    _run_allocator(camera_queue, camera_condition, camera_sem, "Camera")

def door_allocator():
    _run_allocator(door_queue, door_condition, door_sem, "Door")

def release_light():
    light_sem.release()
    with light_condition:
        light_condition.notify_all()

def release_camera():
    camera_sem.release()
    with camera_condition:
        camera_condition.notify_all()

def release_door():
    door_sem.release()
    with door_condition:
        door_condition.notify_all()

def enqueue_device(queue, condition, user_name):
    """Add user to a device FIFO queue. Returns an Event to wait on."""
    event = threading.Event()
    with condition:
        queue.append((user_name, event))
        condition.notify_all()
    return event


# ===================================================
# MAIN USER THREAD (smart_home)
# ===================================================

def smart_home(user, role):
    global light_status, door_status, camera_status
    global total_served, granted_count, denied_count

    user_start = time.time()

    with system_lock:
        active_users.append(f"{user} ({role})")

    tprint(f"\n{'='*62}")
    tprint(f"[{current_time()}] +T={elapsed()}s | NEW USER: {user} | Role: {role}")
    tprint(f"{'='*62}")
    add_log(f"{user} ({role}) entered the system")

    # MOTION SENSOR
    tprint(f"  [{user}] Motion sensor triggered")
    add_log(f"{user}: motion detected")
    time.sleep(0.2)

    # LIGHT
    tprint(f"  [{user}] Requesting LIGHT (joining queue)...")
    add_log(f"{user}: queued for light")
    light_event = enqueue_device(light_queue, light_condition, user)
    light_event.wait()
    with system_lock:
        light_status = "ON"
    tprint(f"  [{user}] +T={elapsed()}s | LIGHT ON (FIFO granted)")
    add_log(f"{user}: light ON")
    time.sleep(LIGHT_USE_TIME)
    with system_lock:
        light_status = "OFF"
    tprint(f"  [{user}] LIGHT OFF")
    add_log(f"{user}: light OFF")
    release_light()

    # CAMERA
    tprint(f"  [{user}] Requesting CAMERA (joining queue)...")
    add_log(f"{user}: queued for camera")
    camera_event = enqueue_device(camera_queue, camera_condition, user)
    camera_event.wait()
    with system_lock:
        camera_status = "ON"
    tprint(f"  [{user}] +T={elapsed()}s | CAMERA recording (FIFO granted)")
    add_log(f"{user}: camera recording started")
    time.sleep(CAMERA_REC_TIME)
    with system_lock:
        camera_status = "OFF"
    tprint(f"  [{user}] CAMERA stopped")
    add_log(f"{user}: camera recording stopped")
    release_camera()

    # DOOR
    tprint(f"  [{user}] Requesting DOOR ACCESS (joining queue)...")
    add_log(f"{user}: queued for door")
    door_event = enqueue_device(door_queue, door_condition, user)
    door_event.wait()
    tprint(f"  [{user}] +T={elapsed()}s | DOOR control granted (FIFO)")
    with system_lock:
        if role == "Family":
            authorized = True
        else:
            authorized = (hash(user) % 2 == 0)
        if authorized:
            door_status = "UNLOCKED"
            granted_count += 1
        else:
            denied_count += 1
    if authorized:
        tprint(f"  [{user}] ACCESS GRANTED - Door UNLOCKED")
        add_log(f"{user}: access GRANTED, door unlocked")
    else:
        tprint(f"  [{user}] ACCESS DENIED")
        add_log(f"{user}: access DENIED")
    time.sleep(DOOR_HOLD_TIME)
    with system_lock:
        door_status = "LOCKED"
    tprint(f"  [{user}] Door re-LOCKED")
    add_log(f"{user}: door re-locked")
    release_door()

    # WRAP UP
    user_elapsed = round(time.time() - user_start, 1)
    with system_lock:
        active_users.remove(f"{user} ({role})")
        total_served += 1
        served_users.append(f"{user} ({role})")
        completion_times.append((user, user_elapsed))
    tprint(f"  [{user}] Process complete | Total time: {user_elapsed}s\n")
    add_log(f"{user}: completed in {user_elapsed}s")


# ===================================================
# DASHBOARD
# ===================================================

def dashboard():
    with system_lock:
        ls          = light_status
        ds          = door_status
        cs          = camera_status
        users_now   = list(active_users)
        total       = total_served
        granted     = granted_count
        denied      = denied_count
        served      = list(served_users)

    line()
    print("  SMART HOME - LIVE STATUS")
    line()
    print(f"  Light            : {ls}")
    print(f"  Door             : {ds}")
    print(f"  Camera           : {cs}")
    print(f"  Active Users     : {len(users_now)}")
    print(f"  Total Served     : {total}")
    print(f"  Access Granted   : {granted}")
    print(f"  Access Denied    : {denied}")
    if users_now:
        print("  Currently Active :")
        for u in users_now:
            print(f"    . {u}")
    else:
        print("  Currently Active : None")
    if served:
        print("\n  Served Users:")
        for i, u in enumerate(served, 1):
            print(f"    {i}. {u}")
    line()


# ===================================================
# TIME ANALYSIS REPORT
# ===================================================

def time_analysis():
    with system_lock:
        times = list(completion_times)
        total = total_served

    line()
    print("  TIME ANALYSIS REPORT")
    line()
    per_user_time = LIGHT_USE_TIME + CAMERA_REC_TIME + DOOR_HOLD_TIME
    print(f"  Fixed times per user:")
    print(f"    Light ON duration   : {LIGHT_USE_TIME}s")
    print(f"    Camera recording    : {CAMERA_REC_TIME}s")
    print(f"    Door hold time      : {DOOR_HOLD_TIME}s")
    print(f"    Total (sequential)  : {per_user_time}s per user")
    print()
    if times:
        print(f"  Actual completion times:")
        for user, t in times:
            print(f"    {user:<20} {t}s")
        fastest = min(t for _, t in times)
        slowest = max(t for _, t in times)
        total_sys = round(time.time() - system_start_time, 1) if system_start_time else 0
        sequential_baseline = total * per_user_time
        print()
        print(f"  Fastest user        : {fastest}s")
        print(f"  Slowest user        : {slowest}s")
        print(f"  System elapsed      : {total_sys}s")
        print(f"  Sequential baseline : {sequential_baseline}s")
        if total_sys > 0 and sequential_baseline > 0:
            improvement = round((1 - total_sys / sequential_baseline) * 100, 1)
            print(f"  Time saved          : {improvement}% over fully sequential")
    else:
        print("  No users served yet.")
    line()


# ===================================================
# LOGS
# ===================================================

def view_logs():
    with system_lock:
        logs_copy = list(logs)
    line()
    print("  ACTIVITY LOGS")
    line()
    if not logs_copy:
        print("  No logs yet.")
    else:
        for entry in logs_copy:
            print(f"  {entry}")
    line()


# ===================================================
# MENU
# ===================================================

def show_menu():
    line()
    print("  IOT SMART HOME - LIGHT AND MOTION SENSOR LOCK")
    line()
    print("  1. Add User (concurrent - multiple allowed)")
    print("  2. Home Status Dashboard")
    print("  3. Activity Logs")
    print("  4. Time Analysis Report")
    print("  5. Exit")
    line()


# ===================================================
# MAIN
# ===================================================

if __name__ == "__main__":
    # Start allocator daemon threads (one per device)
    for fn in (light_allocator, camera_allocator, door_allocator):
        t = threading.Thread(target=fn, daemon=True)
        t.start()

    system_start_time = time.time()

    print("\n  System online. Add multiple users - they run concurrently.")
    print("  Semaphores + FIFO queues handle contention fairly.\n")

    while True:
        show_menu()
        choice = input("  Enter Choice: ").strip()

        if choice == "1":
            name = input("  Enter Name: ").strip()
            if not name:
                print("  Name cannot be empty.")
                continue
            print("  1. Family")
            print("  2. Guest")
            role_choice = input("  Choose Role: ").strip()
            role = "Family" if role_choice == "1" else "Guest"
            t = threading.Thread(target=smart_home, args=(name, role))
            threads.append(t)
            t.start()
            print(f"\n  [{name}] thread started. Add another user or check status.\n")
        elif choice == "2":
            dashboard()
        elif choice == "3":
            view_logs()
        elif choice == "4":
            time_analysis()
        elif choice == "5":
            print("\n  Closing system - waiting for all users to finish...")
            for t in threads:
                t.join()
            print("\n  --- FINAL TIME ANALYSIS ---")
            time_analysis()
            print("  System closed.\n")
            break
        else:
            print("  Invalid choice. Try again.")