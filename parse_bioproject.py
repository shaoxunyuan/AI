#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
parse_prjna_to_excel.py）

功能：
1. 通过 PRJNA 号自动获取 BioProject / GEO / PubMed / SRA 元信息；
2. 调用 DeepSeek 解析疾病大类(ICD-11)、样本来源与分组；
3. 输出 Excel 文件 (metadata, bioproject, sampletable, grouping_rules)；
4. bioproject sheet 新增 journal_name 和 publication_year。
"""

import os
import sys
import json
import re
import subprocess
from io import StringIO
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime

import pandas as pd
import requests

API_KEY = os.getenv("DEEPSEEK_API_KEY")
API_URL = "https://api.deepseek.com/v1/chat/completions"
MODEL = "deepseek-chat"
TIMEOUT = 120


# ================= 工具函数 =================

def sh(cmd, check=True):
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def ensure_cmd(cmd_name: str, hint: str = ""):
    try:
        sh([cmd_name, "--version"])
    except Exception:
        msg = f"找不到命令：{cmd_name}. {hint}" if hint else f"找不到命令：{cmd_name}."
        raise RuntimeError(msg)


def deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """去除重复或全空列"""
    if df is None or df.empty:
        return df
    df = df.copy()
    df = df.dropna(axis=1, how="all")
    nunique = df.nunique(dropna=False)
    df = df.loc[:, nunique > 1]
    seen, keep = {}, []
    for c in df.columns:
        key = tuple(df[c].astype(str).fillna("NA").values)
        if key in seen:
            continue
        seen[key] = c
        keep.append(c)
    return df[keep]


def ask_deepseek(prompt: str) -> str:
    """调用 DeepSeek 接口"""
    if not API_KEY:
        raise RuntimeError("请先设置环境变量 DEEPSEEK_API_KEY")
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    data = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "You are a bioinformatics expert skilled in parsing SRA/GEO metadata and extracting structured study information."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    resp = requests.post(API_URL, headers=headers, json=data, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def run_r_script(script_name: str, args: List[str]) -> str:
    script_path = Path(__file__).resolve().parent / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"找不到 R 脚本: {script_path}")
    res = sh(["Rscript", str(script_path)] + args)
    if res.returncode != 0:
        raise RuntimeError(res.stderr)
    return res.stdout.strip()


def fetch_bioproject_fields(prj_id: str) -> Dict:
    try:
        out = run_r_script("bioproject_extract.R", [prj_id])
        return json.loads(out)
    except Exception as e:
        print(f"⚠️ BioProject 提取失败：{e}")
        return {}


def fetch_geo_pubmed(geo_id: Optional[str] = None) -> Dict:
    geo_id = (geo_id or "").strip()
    try:
        out = run_r_script("geo_pubmed_extract.R", [geo_id])
        return json.loads(out)
    except Exception as e:
        print(f"⚠️ GEO/PubMed 提取失败：{e}")
        return {"pubmed_id": None, "pubmed_title": None, "pubmed_journal": None, "pubmed_date": None}


def get_metadata_with_pysradb(prj_id: str) -> pd.DataFrame:
    print(f"📥 获取 {prj_id} 的 metadata (--detailed --expand)...")
    res = sh(["pysradb", "metadata", prj_id, "--detailed", "--expand"])
    df = pd.read_csv(StringIO(res.stdout), sep="\t", dtype=str)
    print(f"✅ 成功获取 {len(df)} 条记录")
    return df


def strip_download_cols(df: pd.DataFrame) -> pd.DataFrame:
    """删除下载相关列（URL、MD5等）"""
    if df is None or df.empty:
        return df
    drop_keys = ["ftp", "http", "url", "aspera", "download", "md5", "fastq", "bam", "cram", "sra_file"]
    to_drop = [c for c in df.columns if any(k in c.lower() for k in drop_keys)]
    return df.drop(columns=to_drop, errors="ignore")


def normalize_group_label(s: str) -> str:
    """格式规范化：去掉 group、规范 dayN"""
    if not s or str(s).upper() == "NA":
        return "NA"
    v = str(s).strip()
    v = re.sub(r"\bgroup\b", "", v, flags=re.I)
    v = re.sub(r"第?\s*(\d+)\s*天", r"day\1", v)
    v = re.sub(r"time(point)?\s*(\d+)", r"day\2", v, flags=re.I)
    return v.strip()


def select_grouping_candidate_cols(df: pd.DataFrame) -> List[str]:
    """筛选可能代表分组的列"""
    if df is None or df.empty:
        return []
    n = len(df)
    exclude_keys = ["acc", "accession", "run", "srr", "srx", "srs", "sra", "gsm", "samn",
                    "ftp", "http", "url", "md5", "download", "size"]
    cands = []
    for c in df.columns:
        lc = c.lower()
        if any(k in lc for k in exclude_keys):
            continue
        k = df[c].astype(str).fillna("NA").nunique()
        if 2 <= k <= min(10, max(2, n // 2)):
            cands.append(c)
    return cands


# ================= 主流程 =================

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("prj_id", help="PRJNA号，如 PRJNA979185")
    parser.add_argument("--outdir", default=".", help="输出目录")
    args = parser.parse_args()

    prj_id = args.prj_id
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    xlsx_path = outdir / f"{prj_id}_metadata.xlsx"
    prompt_path = outdir / f"{prj_id}_deepseek_prompt.txt"

    ensure_cmd("pysradb", "请先安装 pysradb：pip install pysradb")
    ensure_cmd("Rscript", "需要 Rscript 和 R 包 GEOquery/rentrez/xml2/jsonlite")

    print("\n[1/4] 获取 BioProject / GEO / PubMed 信息...")
    bio_fields = fetch_bioproject_fields(prj_id)
    geo_id = (bio_fields.get("geo_accession") or "").strip()
    geo_pub = fetch_geo_pubmed(geo_id)

    def join_clean(values, sep=", "):
        """去重且保持顺序地拼接多值；过滤空值/NA"""
        seen = set()
        out = []
        for v in values:
            if v is None:
                continue
            s = str(v).strip()
            if s == "" or s.upper() == "NA":
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
        return sep.join(out) if out else "NA"

    # ===== 支持多个 PubMed 的返回结构 =====
    if isinstance(geo_pub, dict) and geo_pub and all(isinstance(v, dict) for v in geo_pub.values()):
        # 形如 {"39160575": {...}, "39812345": {...}}
        pubmed_ids = list(geo_pub.keys())
        journals   = [v.get("journal") for v in geo_pub.values()]
        dates      = [
            v.get("pub_date") or v.get("publication_date") or v.get("pubmed_date")
            for v in geo_pub.values()
        ]
        dois       = [v.get("doi") or v.get("pubmed_doi") for v in geo_pub.values()]

        pubmed_id  = join_clean(pubmed_ids, sep=",")
        pub_journal= join_clean(journals,   sep="; ")
        pub_date   = join_clean(dates,      sep="; ")
        pub_doi    = join_clean(dois,       sep="; ")
    else:
        # 单篇 PubMed 的旧结构
        pubmed_id  = (geo_pub.get("pubmed_id") if isinstance(geo_pub, dict) else None) or "NA"
        pub_journal= (geo_pub.get("journal") or geo_pub.get("pubmed_journal")) or "NA"
        pub_date   = (geo_pub.get("pub_date") or geo_pub.get("publication_date") or geo_pub.get("pubmed_date")) or "NA"
        pub_doi    = (geo_pub.get("doi") or geo_pub.get("pubmed_doi")) or "NA"

    print(f"✅ {prj_id} / {geo_id or 'NA'} / {pubmed_id or 'NA'}")


    
    print("\n[2/4] 获取 pysradb metadata 并清理...")
    df_meta_full = get_metadata_with_pysradb(prj_id).fillna("NA")
    df_meta = strip_download_cols(df_meta_full)

    print("\n[3/4] 构建 DeepSeek 提示词并调用...")
    df_clean = deduplicate_columns(df_meta)
    preview = pd.DataFrame([
        {"Column": c, "UniqueN": df_clean[c].nunique(), "Examples": df_clean[c].astype(str).unique()[:6].tolist()}
        for c in df_clean.columns
    ])

    prompt = f"""
# DeepSeek Prompt
# PRJNA: {prj_id}
# Generated at {datetime.now().isoformat(timespec="seconds")}

Goal:
From BioProject / GEO / PubMed / SRA metadata, extract key study information and output JSON (no explanation).

Output JSON format:
{{
  "disease_major": "ICD-11 chapter name (English)",
  "disease_minor": "specific disease name (English, e.g., COVID-19)",
  "icd11_code": "ICD-11 code if available, else NA",
  "sample_source": "sample origin in English (e.g., PBMC, serum, lung tissue)",
  "grouping_columns": [
    {{
      "column_name": "metadata column name",
      "grouping_logic": {{"value or regex:pattern": "GroupName(EN)"}},
      "confidence": "High/Medium/Low",
      "reason": "short reasoning (Chinese allowed)"
    }}
  ]
}}

Constraints:
- All output must be in English.
- disease_major should correspond to an ICD-11 chapter name.
- Do NOT include 'group' in group names.
- Timepoints should use dayN format (e.g., day7, day14).

BioProject:
{json.dumps(bio_fields, ensure_ascii=False, indent=2)}

PubMed:
{json.dumps(geo_pub, ensure_ascii=False, indent=2)}

SRA columns preview (deduplicated):
{preview.to_string(index=False)}
"""
    prompt_path.write_text(prompt, encoding="utf-8")
    print(f"📝 已保存提示词到 {prompt_path}")

    disease_major = disease_minor = icd11_code = sample_source = "NA"
    grouping = []
    try:
        analysis = ask_deepseek(prompt)
        m = re.search(r"\{.*\}", analysis, re.S)
        if m:
            parsed = json.loads(m.group())
            disease_major = (parsed.get("disease_major") or "NA")
            disease_minor = (parsed.get("disease_minor") or "NA")
            icd11_code    = (parsed.get("icd11_code") or "NA")
            sample_source = (parsed.get("sample_source") or "NA")
            grouping      = parsed.get("grouping_columns") or []
    except Exception as e:
        print(f"⚠️ DeepSeek 调用失败: {e}")

    # ====== 生成 sampletable ======
    run_col = next((c for c in df_meta.columns if re.search(r"(^|_)run(_|$)|run_accession", c, re.I)), None)
    bio_col = next((c for c in df_meta.columns if "biosample" in c.lower()), None)

    df_sample = pd.DataFrame({
        "run_accession": df_meta_full[run_col] if run_col else "NA",
        "biosample": df_meta_full[bio_col] if bio_col else "NA",
    }).copy()

    if grouping:
        for i, rule in enumerate(grouping):
            cname = rule.get("column_name")
            logic = rule.get("grouping_logic", {}) or {}
            out_col = "group" if i == 0 else f"subgroup{i}"
            df_sample[out_col] = "NA"
            if cname and cname in df_meta_full.columns:
                colvals = df_meta_full[cname].astype(str)
                for patt, grp in logic.items():
                    if patt.startswith("regex:"):
                        pat = patt[6:]
                        mask = colvals.str.contains(pat, case=False, na=False)
                    else:
                        mask = colvals == str(patt)
                    df_sample.loc[mask, out_col] = normalize_group_label(grp)

    keep_group_cols = select_grouping_candidate_cols(df_meta)
    ref_cols_df = df_meta[keep_group_cols] if keep_group_cols else pd.DataFrame(index=df_meta.index)
    sampletable_df = pd.concat([df_sample, ref_cols_df], axis=1)

    # ====== bioproject 工作表 ======
    # 提取期刊和年份
    # 使用上面识别到的 journal / date / doi
    pub_journal = pub_journal or geo_pub.get("journal") or "NA"
    pub_date = pub_date or geo_pub.get("pub_date") or geo_pub.get("publication_date") or "NA"

    group_info = "NA"
    if "group" in sampletable_df.columns:
        counts = sampletable_df["group"].value_counts(dropna=False).to_dict()
        counts.pop("NA", None)
        if counts:
            group_info = "; ".join([f"{k}: {v}" for k, v in counts.items()])

    def first_non_na(col):
        return (df_meta[col][df_meta[col] != "NA"].iloc[0] if col in df_meta.columns and (df_meta[col] != "NA").any() else "NA")

    instrument       = first_non_na("instrument")
    library_strategy = first_non_na("library_strategy")
    library_source   = first_non_na("library_source")
    library_selection= first_non_na("library_selection")
    library_layout   = first_non_na("library_layout")

    bioproject_rows = [
        ("bioproject", prj_id),
        ("geo_accession", geo_id or "NA"),
        ("pmid", pubmed_id or "NA"),
        ("journal_name", pub_journal or "NA"),
        ("publication_data", pub_date or "NA"),   # 你之前列名是 publication_data，就沿用
        ("publication_doi", pub_doi or "NA"),
        # 下面保持不变
        ("species", bio_fields.get("organism_name") or "NA"),
        ("disease_major", disease_major),
        ("disease_minor", disease_minor),
        ("icd11_code", icd11_code),
        ("sample_source", sample_source),
        ("instrument", instrument),
        ("library_strategy", library_strategy),
        ("library_source", library_source),
        ("library_selection", library_selection),
        ("library_layout", library_layout),
        ("grouping", ", ".join([r.get("column_name") for r in grouping]) if grouping else "NA"),
        ("group_info", group_info),
        ("sample_size", str(sampletable_df["biosample"].nunique()
                            if "biosample" in sampletable_df.columns
                            else len(sampletable_df)))
    ]

    bioproject_df = pd.DataFrame(bioproject_rows, columns=["字段名", "内容"])

    # ====== grouping_rules ======
    rules_df = None
    if grouping:
        rows = []
        for col in grouping:
            rows.append({
                "column_name": col.get("column_name"),
                "grouping_logic": json.dumps(col.get("grouping_logic", {}), ensure_ascii=False),
                "confidence": col.get("confidence"),
                "reason": col.get("reason"),
            })
        rules_df = pd.DataFrame(rows)

    print("\n[4/4] 写出 Excel 文件...")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        df_meta.to_excel(w, sheet_name="metadata", index=False)
        bioproject_df.to_excel(w, sheet_name="bioproject", index=False)
        sampletable_df.to_excel(w, sheet_name="sampletable", index=False)
        if rules_df is not None:
            rules_df.to_excel(w, sheet_name="grouping_rules", index=False)

    print(f"✅ 完成：{xlsx_path}")
    print("📑 工作表：metadata, bioproject, sampletable, grouping_rules(如有)")
    print(f"📝 DeepSeek 提示词：{prompt_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 运行失败：{e}")
        sys.exit(1)
