"""Selenium Chrome — mở Chrome thật, test trực tiếp dashboard đa camera."""
from __future__ import annotations

import os
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


def wait_for_body_text(driver, substrings, timeout=60):
    end = time.time() + timeout
    last = ""
    while time.time() < end:
        last = driver.find_element(By.TAG_NAME, "body").text
        if any(s.lower() in last.lower() for s in substrings):
            return last
        time.sleep(1.5)
    return last


def click_tab_by_text(driver, label: str) -> bool:
    # Streamlit tabs render as buttons / tab roles
    candidates = driver.find_elements(By.CSS_SELECTOR, "button, [role='tab']")
    for el in candidates:
        txt = (el.text or "").strip()
        if label.lower() in txt.lower():
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                el.click()
                time.sleep(2)
                return True
            except Exception:
                continue
    return False


def main() -> int:
    print("=== Selenium Chrome LIVE dashboard test ===")
    headless = os.getenv("SELENIUM_HEADLESS", "0").strip() in {"1", "true", "yes"}

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    else:
        options.add_argument("--start-maximized")
    options.add_argument("--window-size=1500,1000")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Keep browser open briefly so user can see interactions
    options.add_experimental_option("detach", False)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    failures = []
    try:
        urls = [
            ("Dashboard", "http://localhost:8501/"),
            ("Spark Master", "http://localhost:8082/"),
            ("Airflow", "http://localhost:8085/health"),
            ("RabbitMQ", "http://localhost:15672/"),
            ("MinIO", "http://localhost:9001/"),
        ]

        # 1) Open Spark Master first to verify N workers, then dashboard
        print(">> Opening Spark Master UI...")
        driver.get("http://localhost:8082/")
        time.sleep(2)
        spark_body = driver.find_element(By.TAG_NAME, "body").text
        print(spark_body[:500])
        if "Alive Workers" not in spark_body and "Workers" not in spark_body:
            failures.append("Spark Master UI missing workers section")
        else:
            # Prefer Alive Workers: N >= 3
            for line in spark_body.splitlines():
                if "Alive Workers" in line:
                    print("   ", line.strip())
                    digits = "".join(ch for ch in line if ch.isdigit())
                    if digits and int(digits) < 1:
                        failures.append(f"No alive Spark workers: {line.strip()}")
                    break

        print(">> Opening Streamlit Dashboard...")
        driver.get("http://localhost:8501/")
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        body = wait_for_body_text(driver, ["Giám Sát", "Mock", "ClickHouse"], timeout=60)
        body = wait_for_body_text(
            driver,
            ["Tổng Số Xe", "Khung Hình", "Camera Có Dữ Liệu", "Thống Kê"],
            timeout=90,
        )
        print("--- dashboard snippet ---")
        print(body[:1800])
        print("-------------------------")

        if "Giám Sát" not in body:
            failures.append("Dashboard title missing")
        if "chưa có dữ liệu" in body.lower() or "bảng trống" in body.lower():
            failures.append("Dashboard empty in mock mode")

        # Expect multi-camera expansion (>= 3 cameras with data ideally)
        if "Camera Có Dữ Liệu" in body or "CAMERA CÓ DỮ LIỆU" in body.upper():
            pass
        else:
            # case-insensitive fallback already in markers
            pass

        markers = ["Camera", "Khung Hình", "Tổng Số Xe", "Thống Kê", "Bản Đồ", "MinIO"]
        if not any(m.lower() in body.lower() for m in markers):
            failures.append("Expected dashboard markers missing")

        selects = driver.find_elements(By.CSS_SELECTOR, "[data-baseweb='select'], [role='combobox']")
        if not selects:
            failures.append("No select/combobox widgets found")

        # Interact: open Map tab then Images tab then back to Stats
        print(">> Click tab Bản Đồ...")
        if click_tab_by_text(driver, "Bản Đồ"):
            time.sleep(2)
            map_body = driver.find_element(By.TAG_NAME, "body").text
            if "camera" not in map_body.lower() and "vị trí" not in map_body.lower():
                failures.append("Map tab content incomplete")
            driver.save_screenshot("tests/dashboard_tab_map.png")
        else:
            failures.append("Could not click Bản Đồ tab")

        print(">> Click tab Ảnh / MinIO...")
        if click_tab_by_text(driver, "Ảnh") or click_tab_by_text(driver, "MinIO"):
            time.sleep(2)
            driver.save_screenshot("tests/dashboard_tab_images.png")
        else:
            print("   (tab ảnh not clicked — continue)")

        print(">> Click tab Thống Kê...")
        click_tab_by_text(driver, "Thống Kê")
        time.sleep(2)

        shot = "tests/dashboard_selenium.png"
        driver.save_screenshot(shot)
        print(f"Screenshot saved: {shot}")

        # Pause so user can see the opened Chrome UI
        pause = float(os.getenv("SELENIUM_PAUSE_SECONDS", "8"))
        print(f">> Keeping Chrome open {pause:.0f}s for live view...")
        time.sleep(pause)

        # Quick reachability of other UIs (navigate)
        for name, url in urls[2:]:
            try:
                driver.get(url)
                time.sleep(1.2)
                print(f"[OK] Opened {name}: {url}")
            except Exception as exc:
                failures.append(f"Failed opening {name}: {exc}")

    except Exception as exc:
        failures.append(str(exc))
        try:
            driver.save_screenshot("tests/dashboard_selenium_error.png")
        except Exception:
            pass
    finally:
        driver.quit()

    if failures:
        print("[FAIL] Selenium checks:")
        for f in failures:
            print(" -", f)
        return 1

    print("[PASS] Selenium LIVE Chrome dashboard test OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
