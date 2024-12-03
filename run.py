from pathlib import Path

from tqdm import tqdm

from src.utils import get_configs
from src.etl import load_table_to_stg, transform_to_intermediate, transform_to_mart

STG_CONFIGS = get_configs(path="./config/staging.toml")

def main():
    staging_dir = Path(STG_CONFIGS["staging"]["dir"])
    staging_dir.mkdir(parents=True, exist_ok=True)

    files = STG_CONFIGS["staging"]["files"]
    tables = []

    for file in (pbar := tqdm(files)):
        table_name = file["table_name"]
        file_name = file["file_name"]
        create_if_not_exists = file["create_if_not_exists"]
        drop_if_exists = file["drop_if_exists"]

        path = staging_dir / file_name

        pbar.set_description(f"Processing {file_name}")
        
        if not path.exists():
            Exception(f"File {str(path)} doesn't exist.")

        load_table_to_stg(
            path_to_file=str(path),
            table_name=table_name,
            drop_if_exists=drop_if_exists,
            create_if_not_exists=create_if_not_exists,
        )

        tables.append(table_name)

    transform_to_intermediate(tables=tables)
    transform_to_mart()


if __name__ == "__main__":
    main()
    