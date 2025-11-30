import pandas as pd
import numpy as np
from io import StringIO
import random
from faker import Faker

fake = Faker()

HOUSE_NAMES = ["Griffincrest", "Serpenthelm", "Ravenbrook", "Badgerfen"]

COLORS = {
    "Griffincrest": ["red"],
    "Serpenthelm": ["green"],
    "Ravenbrook": ["blue"],
    "Badgerfen": ["yellow"],
}


def generate_students(num):
    rows = []
    for id_ in range(num):
        first = fake.first_name()
        last = fake.last_name()
        rows.append(
            {
                "id": 1250 + id_,
                "student_id": f"S{fake.unique.random_number(digits=6)}",
                "first_name": first,
                "last_name": last,
                "grade": random.randint(1, 12),
                "email": f"{first.lower()}.{last.lower()}@example.edu",
            }
        )
    return pd.DataFrame(rows)


def generate_houses() -> pd.DataFrame:
    rows = []
    for n in range(len(HOUSE_NAMES)):
        name = HOUSE_NAMES[n]
        rows.append(
            {
                "id": 2300 + n,
                "house_name": name,
                "colors": ", ".join(COLORS[name]),
                "founder_year": random.randint(2000, 2025),
            }
        )
    return pd.DataFrame(rows)


def generate_races(n: int) -> pd.DataFrame:
    rows = []
    adjectives = ["Lightning", "Thunder", "Blazing", "Golden", "Rapid", "Flying"]
    nouns = ["Dash", "Quest", "Match", "Sprint", "Tournament", "Cup"]

    for id_ in range(n):
        rows.append(
            {
                "id": id_ + 1,
                "name": f"{random.choice(adjectives)} {random.choice(nouns)}",
            }
        )
    return pd.DataFrame(rows)


def generate_placements(races, houses, min=1, max=6):
    results = []

    for _, race_row in races.iterrows():
        race_id = race_row["id"]
        race_name = race_row["name"]

        n_participants = np.random.randint(min, max)
        participating_houses = np.random.choice(
            houses["id"], size=n_participants, replace=True
        )

        # Shuffle for placements
        placements = np.random.permutation(participating_houses)
        for place, house_id in enumerate(placements, start=1):
            results.append(
                {
                    "race_id": race_id,
                    "house_id": house_id,
                    "place": place,
                }
            )
    return pd.DataFrame(results)


def generate_housepoints():
    students = generate_students(20)
    houses = generate_houses()

    yield ("students", students)

    students["house_id"] = np.random.choice(houses["id"], size=len(students))

    assoc_table = students[["id", "house_id"]]

    yield ("houses", houses)
    yield ("students_houses", assoc_table)

    races = generate_races(50)
    yield ("races", races)
    yield ("placements", generate_placements(races, houses))
