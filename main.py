def main():
  import polars as pl
  import datetime as dt
  import os

  df = pl.DataFrame(
      {
          "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
          "birthdate": [
              dt.date(1997, 1, 10),
              dt.date(1985, 2, 15),
              dt.date(1983, 3, 22),
              dt.date(1981, 4, 30),
          ],
          "weight": [57.9, 72.5, 53.6, 83.1],  # (kg)
          "height": [1.56, 1.77, 1.65, 1.75],  # (m)
      }
  )

  python_polars_path = os.path.join(os.path.expanduser("~"), ".python-polars")
  os.mkdir(python_polars_path)

  df.write_csv(python_polars_path + "/df.csv")
  df_csv = pl.read_csv(python_polars_path + "/df.csv", try_parse_dates = True)
  print(df_csv)

if __name__ == "__main__":
    main()