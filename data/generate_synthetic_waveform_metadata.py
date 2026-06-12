import csv
from datetime import datetime, timedelta

N_SUBJECTS = 20
BASE_YEAR_REAL = 2045
BASE_YEAR_MIMIC = 2065

CHANNEL_PAIRS = [
    ("PLETH", "NU", 250, "adu/NU"),
    ("RESP", "Ohm", 210, "adu/Ohm"),
]

ECG_LEADS = ["II", "III", "V", "aVR"]

def dt(y, m, d, h=0, mi=0, s=0):
    return datetime(y, m, d, h, mi, s)

def fmt(t):
    return t.strftime("%Y-%m-%d %H:%M:%S.000000 UTC")

def choose_channels(i, segment_idx):
    base = CHANNEL_PAIRS[(i + segment_idx) % len(CHANNEL_PAIRS)]
    ecg = ECG_LEADS[(i + segment_idx) % len(ECG_LEADS)]
    return [
        (base[0], base[1], base[2], base[3]),
        (ecg, "mV", 125, "adu/mV"),
    ]

waveform_files = []
waveform_channels = []

for i in range(1, N_SUBJECTS + 1):
    subject_id = 20000000 + i
    person_id = 9000000000 + i

    # decide structure
    n_recordings = 2 if i in (4, 10) else 1
    multi_segment = i in (3, 5)

    for r in range(n_recordings):
        rec_idx = i * 1000 + r
        hadm_id = 84000000 + rec_idx
        visit_occ = 9500000000 + rec_idx
        visit_detail = 9600000000 + rec_idx
        group_id = 9001000 + rec_idx

        folder_prefix = 80 + (i % 5)
        waveform_folder = f"{folder_prefix}/{group_id}"
        location = ["icu", "ccu"][i % 2]

        t0_real = dt(BASE_YEAR_REAL + i % 5, i % 12 + 1, i % 28 + 1, i % 24)
        t0_mimic = dt(BASE_YEAR_MIMIC + i % 5, i % 12 + 1, i % 28 + 1, i % 24)

        session_duration = timedelta(hours=8)
        session_end_real = t0_real + session_duration
        session_end_mimic = t0_mimic + session_duration

        # --- master header ---
        waveform_files.append([
            subject_id, person_id, hadm_id, visit_occ, visit_detail,
            location, waveform_folder, group_id, group_id,
            fmt(t0_mimic), fmt(session_end_mimic),
            fmt(t0_real), fmt(session_end_real),
            fmt(t0_real), fmt(session_end_real - timedelta(minutes=1)),
            f"{waveform_folder}/{group_id}.hea",
            f"{person_id}/Waveforms/{group_id}/{group_id}.hea"
        ])

        # segments
        n_segments = 2 if multi_segment else 1

        seg_start = t0_real + timedelta(minutes=1)

        for s in range(1, n_segments + 1):
            seg_len_sec = 60 + (s * 30)  # 60s, 90s
            seg_end = seg_start + timedelta(seconds=seg_len_sec)

            seg_name = f"{group_id}_{s:04d}"

            for ext in ["hea", "dat"]:
                waveform_files.append([
                    subject_id, person_id, hadm_id, visit_occ, visit_detail,
                    location, waveform_folder, group_id, group_id,
                    fmt(t0_mimic), fmt(session_end_mimic),
                    fmt(seg_start), fmt(seg_end),
                    fmt(seg_start), fmt(seg_end),
                    f"{waveform_folder}/{seg_name}.{ext}",
                    f"{person_id}/Waveforms/{group_id}/{seg_name}.{ext}"
                ])

            # channels (4 rows per segment)
            segment_length = int(seg_len_sec * 125)

            channels = choose_channels(i, s)

            for ext in ["dat", "hea"]:
                for ch_name, unit, gain, gain_unit in channels:
                    waveform_channels.append([
                        person_id, visit_occ, visit_detail, group_id,
                        fmt(t0_real), fmt(session_end_real),
                        fmt(seg_start), fmt(seg_end),
                        f"{waveform_folder}/{seg_name}.{ext}",
                        f"{person_id}/Waveforms/{group_id}/{seg_name}.{ext}",
                        ch_name,
                        unit,
                        125,
                        "Hz",
                        gain,
                        gain_unit,
                        segment_length
                    ])

            seg_start = seg_end  # next segment starts immediately

# --- write files ---
with open("waveform_files_all.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "subject_id","person_id","hadm_id","visit_occurrence_id","visit_detail_id",
        "location","waveform_folders","record_id","group_id",
        "mimic_start","mimic_end","session_start","session_end",
        "file_start","file_end","src_file","trg_file"
    ])
    writer.writerows(waveform_files)

with open("waveform_channels_all.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "person_id","visit_occurrence_id","visit_detail_id","group_id",
        "session_start","session_end","file_start","file_end",
        "src_file","trg_file","channel_name","sample_units",
        "sample_rate","sample_rate_units","gain","gain_units","segment_length"
    ])
    writer.writerows(waveform_channels)

print("Generated waveform_files_all.csv and waveform_channels_all.csv")