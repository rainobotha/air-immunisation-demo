import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).parent

NUM_PATIENTS = 100_000
NUM_PROVIDERS = 2_000

STATES = ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
STATE_WEIGHTS = [32, 26, 20, 7, 10, 2, 1, 2]

POSTCODES_BY_STATE = {
    "NSW": list(range(2000, 2900)),
    "VIC": list(range(3000, 3900)),
    "QLD": list(range(4000, 4900)),
    "SA":  list(range(5000, 5800)),
    "WA":  list(range(6000, 6900)),
    "TAS": list(range(7000, 7500)),
    "NT":  list(range(800, 900)),
    "ACT": list(range(2600, 2620)),
}

GENDERS = ["M", "F", "X"]
GENDER_WEIGHTS = [49, 49, 2]

INDIGENOUS_STATUS = ["Neither", "Aboriginal", "Torres Strait Islander", "Both", "Not Stated"]
INDIGENOUS_WEIGHTS = [92, 3, 1, 0.5, 3.5]

FIRST_NAMES_M = ["James", "William", "Oliver", "Jack", "Noah", "Thomas", "Henry", "Leo", "Charlie", "Lucas",
                  "Ethan", "Alexander", "Liam", "Mason", "Benjamin", "Samuel", "Daniel", "Matthew", "Joseph", "David",
                  "Archer", "Hudson", "Harvey", "Theodore", "Harrison", "Lachlan", "Cooper", "Hunter", "Riley", "Elijah"]
FIRST_NAMES_F = ["Charlotte", "Olivia", "Amelia", "Isla", "Ava", "Mia", "Grace", "Willow", "Harper", "Chloe",
                  "Ella", "Sophie", "Emily", "Zoe", "Lily", "Matilda", "Evelyn", "Ruby", "Ivy", "Aria",
                  "Layla", "Sienna", "Scarlett", "Audrey", "Hazel", "Luna", "Violet", "Stella", "Penelope", "Poppy"]
LAST_NAMES = ["Smith", "Jones", "Williams", "Brown", "Wilson", "Taylor", "Johnson", "White", "Martin", "Anderson",
              "Thompson", "Thomas", "Walker", "Harris", "Lee", "Ryan", "Robinson", "Kelly", "King", "Davis",
              "Wright", "Lewis", "Hill", "Scott", "Green", "Adams", "Baker", "Hall", "Allen", "Young",
              "Clark", "Mitchell", "Roberts", "Campbell", "Turner", "Phillips", "Parker", "Edwards", "Collins", "Stewart",
              "Murphy", "Morris", "Rogers", "Cook", "Morgan", "Cooper", "Peterson", "Bailey", "Reed", "Bell"]

PROVIDER_TYPES = ["General Practice", "Community Health Centre", "Hospital", "Pharmacy", "Aboriginal Medical Service",
                  "School Program", "Local Health District", "State Health Dept"]
PROVIDER_TYPE_WEIGHTS = [50, 15, 10, 10, 5, 5, 3, 2]

PRACTICE_PREFIXES = ["Sunshine", "Greenfield", "Lakeside", "Mountain", "Coastal", "Valley", "Northern", "Southern",
                     "Eastern", "Western", "Central", "Metro", "Regional", "Heritage", "Pioneer", "Harbour",
                     "Riverside", "Hilltop", "Parkview", "Bayview"]
PRACTICE_SUFFIXES = ["Medical Centre", "Health Clinic", "Family Practice", "Medical Group", "Health Hub",
                     "Community Health", "Medical Practice", "Health Centre", "Doctors", "GP Clinic"]

NIP_SCHEDULE = [
    {"age_months": 0,  "vaccine_brand": "Engerix-B Paediatric",    "antigen": "Hepatitis B",           "dose": 1},
    {"age_months": 2,  "vaccine_brand": "Infanrix Hexa",           "antigen": "DTPa-Hib-IPV-HepB",    "dose": 1},
    {"age_months": 2,  "vaccine_brand": "Prevenar 13",             "antigen": "Pneumococcal (PCV13)",  "dose": 1},
    {"age_months": 2,  "vaccine_brand": "Rotarix",                 "antigen": "Rotavirus",             "dose": 1},
    {"age_months": 4,  "vaccine_brand": "Infanrix Hexa",           "antigen": "DTPa-Hib-IPV-HepB",    "dose": 2},
    {"age_months": 4,  "vaccine_brand": "Prevenar 13",             "antigen": "Pneumococcal (PCV13)",  "dose": 2},
    {"age_months": 4,  "vaccine_brand": "Rotarix",                 "antigen": "Rotavirus",             "dose": 2},
    {"age_months": 6,  "vaccine_brand": "Infanrix Hexa",           "antigen": "DTPa-Hib-IPV-HepB",    "dose": 3},
    {"age_months": 12, "vaccine_brand": "Priorix",                 "antigen": "MMR",                   "dose": 1},
    {"age_months": 12, "vaccine_brand": "Nimenrix",                "antigen": "Meningococcal ACWY",    "dose": 1},
    {"age_months": 12, "vaccine_brand": "Prevenar 13",             "antigen": "Pneumococcal (PCV13)",  "dose": 3},
    {"age_months": 18, "vaccine_brand": "Infanrix/Hiberix",        "antigen": "DTPa-Hib",              "dose": 4},
    {"age_months": 18, "vaccine_brand": "Priorix-Tetra",           "antigen": "MMRV",                  "dose": 1},
    {"age_months": 48, "vaccine_brand": "Infanrix IPV",            "antigen": "DTPa-IPV",              "dose": 5},
    {"age_months": 48, "vaccine_brand": "Priorix",                 "antigen": "MMR",                   "dose": 2},
]

ADULT_VACCINES = [
    {"vaccine_brand": "Fluarix Tetra",      "antigen": "Influenza",     "dose": 1},
    {"vaccine_brand": "Fluarix Tetra",      "antigen": "Influenza",     "dose": 1},
    {"vaccine_brand": "Comirnaty",          "antigen": "COVID-19",      "dose": 1},
    {"vaccine_brand": "Comirnaty",          "antigen": "COVID-19",      "dose": 2},
    {"vaccine_brand": "Comirnaty",          "antigen": "COVID-19",      "dose": 3},
    {"vaccine_brand": "Spikevax",           "antigen": "COVID-19",      "dose": 1},
    {"vaccine_brand": "Spikevax",           "antigen": "COVID-19",      "dose": 2},
    {"vaccine_brand": "Gardasil 9",         "antigen": "HPV",           "dose": 1},
    {"vaccine_brand": "Gardasil 9",         "antigen": "HPV",           "dose": 2},
    {"vaccine_brand": "Boostrix",           "antigen": "DTPa (booster)","dose": 1},
    {"vaccine_brand": "Shingrix",           "antigen": "Herpes Zoster", "dose": 1},
    {"vaccine_brand": "Shingrix",           "antigen": "Herpes Zoster", "dose": 2},
    {"vaccine_brand": "Pneumovax 23",       "antigen": "Pneumococcal (PPV23)", "dose": 1},
]

BATCH_PREFIXES = ["ABV", "FLX", "CVD", "PFZ", "MRK", "GSK", "SNF", "AZN"]


def generate_medicare_number():
    return f"{random.randint(2000, 6999)}{random.randint(10000, 99999)}{random.randint(1, 9)}"


def generate_provider_number():
    return f"{random.randint(100000, 999999)}{random.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}{random.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}"


def generate_batch_number():
    return f"{random.choice(BATCH_PREFIXES)}{random.randint(10000, 99999)}"


def random_date_between(start: date, end: date) -> date:
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))


def introduce_data_quality_issues(value, field_type, probability=0.03):
    if random.random() > probability:
        return value
    issue = random.choice(["null", "typo", "format", "duplicate_space"])
    if issue == "null":
        return ""
    elif issue == "typo" and isinstance(value, str) and len(value) > 2:
        i = random.randint(0, len(value) - 1)
        return value[:i] + random.choice("xzqk") + value[i+1:]
    elif issue == "format" and field_type == "date":
        if isinstance(value, date):
            formats = [value.strftime("%d/%m/%Y"), value.strftime("%m-%d-%Y"), value.strftime("%Y%m%d")]
            return random.choice(formats)
    elif issue == "duplicate_space" and isinstance(value, str):
        words = value.split()
        if len(words) > 1:
            return "  ".join(words)
    return value


print("Generating patients...")
patients = []
for i in range(NUM_PATIENTS):
    state = random.choices(STATES, STATE_WEIGHTS)[0]
    gender = random.choices(GENDERS, GENDER_WEIGHTS)[0]
    if gender == "M":
        first = random.choice(FIRST_NAMES_M)
    elif gender == "F":
        first = random.choice(FIRST_NAMES_F)
    else:
        first = random.choice(FIRST_NAMES_M + FIRST_NAMES_F)

    dob = random_date_between(date(2018, 1, 1), date(2025, 12, 31))
    is_child = (date(2026, 4, 14) - dob).days < 365 * 7
    if random.random() < 0.4:
        dob = random_date_between(date(1950, 1, 1), date(2005, 12, 31))

    patients.append({
        "patient_id": str(uuid.uuid4()),
        "medicare_number": generate_medicare_number(),
        "first_name": introduce_data_quality_issues(first, "string"),
        "last_name": introduce_data_quality_issues(random.choice(LAST_NAMES), "string"),
        "date_of_birth": introduce_data_quality_issues(dob, "date"),
        "gender": gender,
        "indigenous_status": random.choices(INDIGENOUS_STATUS, INDIGENOUS_WEIGHTS)[0],
        "state": state,
        "postcode": str(random.choice(POSTCODES_BY_STATE[state])),
        "address_line1": f"{random.randint(1, 500)} {random.choice(['Smith', 'King', 'George', 'Queen', 'Park', 'High', 'Church', 'Station', 'Bridge', 'Main'])} {random.choice(['Street', 'Road', 'Avenue', 'Drive', 'Place', 'Crescent', 'Lane', 'Way'])}",
        "suburb": random.choice(["Parramatta", "Southbank", "Fortitude Valley", "Adelaide CBD", "Fremantle",
                                 "Hobart", "Darwin", "Canberra", "Bondi", "St Kilda", "Surfers Paradise",
                                 "Glenelg", "Cottesloe", "Salamanca", "Nightcliff", "Manuka",
                                 "Chatswood", "Richmond", "Toowong", "Norwood", "Subiaco",
                                 "Sandy Bay", "Parap", "Kingston", "Penrith", "Geelong",
                                 "Cairns", "Mount Gambier", "Bunbury", "Launceston", "Alice Springs", "Queanbeyan"]),
        "phone": f"04{random.randint(10000000, 99999999)}",
        "email": f"{first.lower()}.{random.choice(LAST_NAMES).lower()}{random.randint(1,999)}@{random.choice(['gmail.com', 'outlook.com', 'yahoo.com.au', 'bigpond.com', 'icloud.com'])}",
    })

print(f"  Generated {len(patients)} patients")


print("Generating providers...")
providers = []
for i in range(NUM_PROVIDERS):
    state = random.choices(STATES, STATE_WEIGHTS)[0]
    ptype = random.choices(PROVIDER_TYPES, PROVIDER_TYPE_WEIGHTS)[0]
    providers.append({
        "provider_id": str(uuid.uuid4()),
        "provider_number": generate_provider_number(),
        "provider_type": ptype,
        "practice_name": f"{random.choice(PRACTICE_PREFIXES)} {random.choice(PRACTICE_SUFFIXES)}",
        "provider_first_name": random.choice(FIRST_NAMES_M + FIRST_NAMES_F),
        "provider_last_name": random.choice(LAST_NAMES),
        "state": state,
        "postcode": str(random.choice(POSTCODES_BY_STATE[state])),
        "phone": f"0{random.choice(['2','3','7','8'])}{random.randint(10000000, 99999999)}",
    })

print(f"  Generated {len(providers)} providers")


print("Generating vaccinations (~500K)...")
vaccinations = []
vax_id = 0
today = date(2026, 4, 14)

for patient in patients:
    dob = patient["date_of_birth"]
    if isinstance(dob, str):
        try:
            dob = date.fromisoformat(dob)
        except ValueError:
            dob = date(2022, 1, 1)

    age_days = (today - dob).days
    is_child = age_days < 365 * 7
    provider = random.choice(providers)
    same_state_providers = [p for p in providers if p["state"] == patient["state"]]
    if same_state_providers:
        provider = random.choice(same_state_providers)

    if is_child:
        compliance_rate = random.random()
        for schedule_item in NIP_SCHEDULE:
            target_date = dob + timedelta(days=schedule_item["age_months"] * 30)
            if target_date > today:
                break
            if compliance_rate < 0.05:
                continue
            if compliance_rate < 0.15 and random.random() < 0.3:
                continue

            actual_date = target_date + timedelta(days=random.randint(-7, 60))
            if actual_date > today:
                actual_date = today - timedelta(days=random.randint(1, 30))

            admin_date = actual_date
            if random.random() < 0.03:
                admin_date = introduce_data_quality_issues(actual_date, "date")

            vax_id += 1
            vaccinations.append({
                "vaccination_id": str(uuid.uuid4()),
                "patient_id": patient["patient_id"],
                "provider_id": provider["provider_id"],
                "vaccine_brand": introduce_data_quality_issues(schedule_item["vaccine_brand"], "string", 0.02),
                "antigen": schedule_item["antigen"],
                "dose_number": schedule_item["dose"],
                "batch_number": generate_batch_number(),
                "administration_date": admin_date if isinstance(admin_date, date) else admin_date,
                "reporting_date": (actual_date + timedelta(days=random.randint(0, 5))) if isinstance(actual_date, date) else actual_date,
                "administration_site": random.choice(["Left arm", "Right arm", "Left thigh", "Right thigh"]),
                "route": random.choice(["Intramuscular", "Subcutaneous", "Oral"]),
                "nip_funded": "Y",
                "school_program": "Y" if schedule_item["age_months"] >= 48 and random.random() < 0.3 else "N",
                "vial_serial_number": "" if "COVID" not in schedule_item["antigen"] else f"VSN{random.randint(100000,999999)}",
            })
    else:
        num_adult_vax = random.choices([0, 1, 2, 3, 4, 5, 6], [10, 15, 25, 25, 15, 7, 3])[0]
        chosen = random.sample(ADULT_VACCINES, min(num_adult_vax, len(ADULT_VACCINES)))
        for vax in chosen:
            admin_date = random_date_between(date(2021, 3, 1), today - timedelta(days=1))
            vax_id += 1
            vaccinations.append({
                "vaccination_id": str(uuid.uuid4()),
                "patient_id": patient["patient_id"],
                "provider_id": random.choice(same_state_providers if same_state_providers else providers)["provider_id"],
                "vaccine_brand": introduce_data_quality_issues(vax["vaccine_brand"], "string", 0.02),
                "antigen": vax["antigen"],
                "dose_number": vax["dose"],
                "batch_number": generate_batch_number(),
                "administration_date": introduce_data_quality_issues(admin_date, "date"),
                "reporting_date": (admin_date + timedelta(days=random.randint(0, 10))),
                "administration_site": random.choice(["Left arm", "Right arm", "Left deltoid", "Right deltoid"]),
                "route": "Intramuscular",
                "nip_funded": random.choice(["Y", "N"]),
                "school_program": "Y" if vax["antigen"] == "HPV" and random.random() < 0.6 else "N",
                "vial_serial_number": f"VSN{random.randint(100000,999999)}" if "COVID" in vax["antigen"] else "",
            })

print(f"  Generated {len(vaccinations)} vaccination records")


print("Injecting duplicate records (~1% of vaccinations)...")
num_dupes = int(len(vaccinations) * 0.01)
dupes = random.sample(vaccinations, num_dupes)
for d in dupes:
    dupe = d.copy()
    dupe["vaccination_id"] = str(uuid.uuid4())
    if random.random() < 0.5:
        dupe["reporting_date"] = dupe["reporting_date"] if isinstance(dupe["reporting_date"], str) else (
            dupe["reporting_date"] + timedelta(days=random.randint(1, 5)) if isinstance(dupe["reporting_date"], date) else dupe["reporting_date"]
        )
    vaccinations.append(dupe)

random.shuffle(vaccinations)
print(f"  Total vaccination records (with dupes): {len(vaccinations)}")


def write_csv(filename, rows, fieldnames):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in rows:
            clean = {}
            for k in fieldnames:
                v = row.get(k, "")
                if isinstance(v, date):
                    v = v.isoformat()
                clean[k] = v
            writer.writerow(clean)
    print(f"  Wrote {path} ({len(rows)} rows)")


print("\nWriting CSVs...")
write_csv("patients.csv", patients, [
    "patient_id", "medicare_number", "first_name", "last_name", "date_of_birth",
    "gender", "indigenous_status", "state", "postcode", "address_line1", "suburb",
    "phone", "email"
])

write_csv("providers.csv", providers, [
    "provider_id", "provider_number", "provider_type", "practice_name",
    "provider_first_name", "provider_last_name", "state", "postcode", "phone"
])

write_csv("vaccinations.csv", vaccinations, [
    "vaccination_id", "patient_id", "provider_id", "vaccine_brand", "antigen",
    "dose_number", "batch_number", "administration_date", "reporting_date",
    "administration_site", "route", "nip_funded", "school_program", "vial_serial_number"
])

print("\nDone! Files written to:", OUTPUT_DIR)
print(f"  patients.csv:      {NUM_PATIENTS:,} rows")
print(f"  providers.csv:     {NUM_PROVIDERS:,} rows")
print(f"  vaccinations.csv:  {len(vaccinations):,} rows")
