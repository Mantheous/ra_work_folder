# Reformat Aube Civil Status CSV to file name and link pairs
import os
csv_path = "Aube_test.csv"
out_path = csv_path.replace(".csv", "_pairs.csv")

for line in open(csv_path, "r", encoding="utf-8"):
    id, commun, period, act_types, image_count, link = line.strip().split("|")
    for i in range(int(image_count)):
        mod_link = link.replace("/0/full", f"/{i}/full")
        # download the file at link and save it as {period}_{act_types}.pdf
        filename = f"Aube_{id}_{commun}_{period}_{act_types}_{i}.pdf".replace("/", "-").replace(" ", "_")
        with open(out_path, "a", encoding="utf-8") as out_file:
            out_file.write(f"{filename}|{mod_link}\n")

print(f"Done! Output saved to {out_path}")