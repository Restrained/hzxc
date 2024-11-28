#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/26 10:52
# @Author  : AllenWan
# @File    : WORK_AT.py
import pandas as pd

def get_work_info(input_file, output_file):
    df = pd.read_csv(input_file)
    df["concatenated"] = df[["authorId", "InstitutionId"]].fillna("").astype(str).apply("§".join, axis=1)
    result = df[["concatenated"]]
    result = result.drop_duplicates(subset=["concatenated"], keep="first")
    result.to_csv(output_file, index=False, header=False, encoding="utf-8-sig")
    return result

if __name__ == "__main__":
    get_work_info(r"D:\output\csv\author_info_id.csv", r"D:\output\csv\WORK_AT.csv")