import json
from hl7apy import parser
from hl7apy.core import Message


def hl7_to_object_fields(hl7_string: str):
    try:
        print("📩 Raw HL7 message:")
        print(hl7_string)

        try:
            msg = parser.parse_message(hl7_string.replace('\n', '\r'), find_groups=True)
        except Exception as e:
            print(f"[⚠️] Group parsing failed: {e} → retrying without group support")
            msg = parser.parse_message(hl7_string.replace('\n', '\r'), find_groups=False)

        # Extract individual fields
        msh_sending_app = msg.msh.msh_3.value if hasattr(msg, "msh") else "N/A"
        pid_patient_id = msg.pid.pid_3.value if hasattr(msg, "pid") else "N/A"
        pid_patient_name = msg.pid.pid_5.value if hasattr(msg, "pid") else "N/A"

        # Print fields
        print("✅ Extracted HL7 Fields:")
        print(f"MSH Sending Application : {msh_sending_app}")
        print(f"PID Patient ID          : {pid_patient_id}")
        print(f"PID Patient Name        : {pid_patient_name}")

        # Convert to full dictionary (HL7 structure)

        if isinstance(msg, Message):
                hl7_dict = msg.to_dict()
                with open("parsed_hl7_message.json", "w") as f:
                        json.dump(hl7_dict, f, indent=2)
                print("📄 JSON saved to parsed_hl7_message.json")
        else:
                print("[⚠️] Parsed HL7 is not a full message. Skipping JSON export.")

        print("📄 JSON saved to parsed_hl7_message.json")

        return True
    except Exception as e:
        print(f"[❌] Failed to parse or access fields: {e}")
        return False
if __name__ == "__main__":
    try:
        with open("sample_hl7.txt", "r") as file:
            hl7_raw = file.read().strip()
        hl7_to_object_fields(hl7_raw)
    except FileNotFoundError:
        print("[❌] File 'sample_hl7.txt' not found.")