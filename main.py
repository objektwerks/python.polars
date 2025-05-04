# See: https://docs.pola.rs/user-guide/getting-started/

def main():
  import polars as pl
  import datetime as dt
  import os

  python_polars_path = os.path.join(os.path.expanduser("~"), ".python-polars")
  os.makedirs(python_polars_path, exist_ok = True)
  df_path = python_polars_path + "/df.csv"

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

  df.write_csv(df_path)
  df_csv = pl.read_csv(df_path, try_parse_dates = True)
  print(df_csv)

  result = df.select(
      pl.col("name"),
      pl.col("birthdate").dt.year().alias("birth_year"),
      (pl.col("weight") / (pl.col("height") ** 2)).alias("bmi"),
  )
  print(result)

if __name__ == "__main__":
    main()