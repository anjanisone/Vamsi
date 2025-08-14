import pandas as pd

def generate_additional_rows(df, annotation_col, auth_col, suffix_range=99):
    new_rows = []
    for _, row in df.iterrows():
        original_annotation = row[annotation_col]
        original_auth = row[auth_col]
        annotation_parts = original_annotation.split("-")
        auth_parts = original_auth.split("-")
        for i in range(suffix_range + 1):
            suffix = f"{i:02d}"
            new_annotation = "-".join(annotation_parts[:4] + [suffix + annotation_parts[4][2:]])
            new_auth = "-".join(auth_parts[:4] + [suffix + auth_parts[4][2:]])
            new_row = row.copy()
            new_row[annotation_col] = new_annotation
            new_row[auth_col] = new_auth
            new_rows.append(new_row)
    return pd.DataFrame(new_rows)
