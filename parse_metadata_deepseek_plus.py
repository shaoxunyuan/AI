#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版：parse_metadata_deepseek_plus.py
功能：
- 调用 bioproject_info_parser.R 获取 BioProject 元信息
- 将结果整合进 DeepSeek prompt，以便模型理解项目背景
- 将 BioProject 信息追加写入 Excel 的 study sheet
- 保留原脚本全部功能
"""

import os
import sys
import requests
import pandas as pd
import subprocess
import json
import re
from io import StringIO

# ========== 配置信息 ==========
API_KEY = os.getenv("DEEPSEEK_API_KEY")
API_URL = "https://api.deepseek.com/v1/chat/completions"

if not API_KEY:
    print("❌ 请先设置环境变量：export DEEPSEEK_API_KEY='你的key'")
    sys.exit(1)


# ========== DeepSeek API 调用函数 ==========
def ask_deepseek(prompt):
    headers = {
        "Authorization": f"Bearer " + API_KEY,
        "Content-Type": "application/json",
    }
    data = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system",
             "content": "你是一名资深生物信息专家，擅长解析 SRA metadata (pysradb) 并提取样本分组逻辑。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2,
    }

    try:
        resp = requests.post(API_URL, headers=headers, json=data, timeout=120)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"❌ DeepSeek API 请求失败: {e}")
        sys.exit(1)


# ========== 调用 bioproject_info_parser.R 获取 BioProject 信息 ==========
def get_bioproject_info(prj_id):
    print(f"📘 正在调用 bioproject_info_parser.R 获取 {prj_id} 的项目信息...")
    try:
        result = subprocess.run(
            ["Rscript", "bioproject_info_parser.R", prj_id],
            capture_output=True, text=True, check=True
        )
        info_text = result.stdout.strip()
        if not info_text:
            info_text = "（无输出或解析失败）"
        print("✅ 成功获取 BioProject 信息")
        return info_text
    except Exception as e:
        print(f"⚠️ 获取 BioProject 信息失败: {e}")
        return "（获取 BioProject 信息失败）"


# ========== 使用 pysradb 获取 metadata ==========
def get_metadata_with_pysradb(prj_id):
    print(f"📥 正在下载 {prj_id} 的 metadata...")
    try:
        cmd = ["pysradb", "metadata", prj_id, "--expand"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        metadata_df = pd.read_csv(StringIO(result.stdout), sep="\t", dtype=str)
        print(f"✅ 成功获取 {len(metadata_df)} 条样本记录")
        return metadata_df
    except Exception as e:
        print(f"❌ 获取 metadata 失败: {e}")
        return None


# ========== 批量规则应用函数 ==========
def apply_grouping_rules(sample_df, rules_dict):
    sample_df = sample_df.copy()
    for col_name, rules in rules_dict.items():
        if col_name not in sample_df.columns:
            print(f"⚠️ 警告: 列 '{col_name}' 不存在，跳过该规则")
            continue
        group_col_name = f"group_{col_name}"
        sample_df[group_col_name] = "NA"
        for value_pattern, group_name in rules.items():
            if value_pattern.startswith("regex:"):
                pattern = value_pattern[6:]
                mask = sample_df[col_name].str.contains(pattern, case=False, na=False)
                sample_df.loc[mask, group_col_name] = group_name
            else:
                mask = sample_df[col_name] == value_pattern
                sample_df.loc[mask, group_col_name] = group_name
    return sample_df


# ========== 主流程 ==========
def main():
    if len(sys.argv) < 2:
        print("用法: python parse_metadata_deepseek_plus.py PRJNA515702")
        sys.exit(1)

    prj_id = sys.argv[1]
    output_file = f"{prj_id}_metadata.xlsx"

    # 检查 pysradb
    try:
        subprocess.run(["pysradb", "--version"], capture_output=True, check=True)
    except Exception:
        print("❌ 未找到 pysradb，请先安装: pip install pysradb")
        sys.exit(1)

    # Step 0️⃣ 获取 BioProject 信息
    bioproject_info_text = get_bioproject_info(prj_id)

    # Step 1️⃣ 获取 metadata
    df = get_metadata_with_pysradb(prj_id)
    if df is None:
        sys.exit(1)
    original_df = df.copy().fillna("NA")

    # Step 2️⃣ study/sample 拆分
    study_cols = [c for c in df.columns if df[c].nunique() == 1]
    sample_cols = [c for c in df.columns if df[c].nunique() > 1]
    study_df = pd.DataFrame({
        "字段名": study_cols,
        "字段值": [df[c].iloc[0] for c in study_cols]
    })
    sample_df = df[sample_cols].fillna("NA").copy()

    # 将 BioProject 信息追加到 study_df
    study_df = pd.concat([
        study_df,
        pd.DataFrame({"字段名": ["BioProject_Info"], "字段值": [bioproject_info_text]})
    ], ignore_index=True)

    # Step 3️⃣ 构造提示词
    preview_data = []
    for col in sample_cols:
        unique_vals = sample_df[col].unique()[:10]
        preview_data.append({
            "列名": col,
            "唯一值数量": sample_df[col].nunique(),
            "示例值": list(unique_vals)
        })
    preview_df = pd.DataFrame(preview_data)

    prompt = f"""
BioProject元信息（来自bioproject_info_parser.R）:
{bioproject_info_text}

以下是SRA样本metadata的列信息，请综合分析样本分组逻辑：
{preview_df.to_string(index=False)}

请基于这些列的数据模式识别分组逻辑。
输出要求：
- 只输出JSON，不要附加解释。
- JSON结构如下：
{{
  "grouping_columns": [
    {{
      "column_name": "列名",
      "grouping_logic": {{
        "字段值或模式1": "分组1",
        "字段值或模式2": "分组2"
      }},
      "confidence": "高/中/低",
      "reason": "理由"
    }}
  ]
}}
"""

    print("🤖 正在调用 DeepSeek 进行分组逻辑分析...")
    analysis_result = ask_deepseek(prompt)
    print("\n=== DeepSeek 分析结果 ===\n")
    print(analysis_result)
    print("========================\n")

    # Step 4️⃣ 解析 AI 输出
    grouping_rules_applied = False
    rules_json = None
    try:
        json_match = re.search(r'\{.*\}', analysis_result, re.DOTALL)
        if json_match:
            rules_json = json.loads(json_match.group())
            rules_dict = {}
            for col_info in rules_json.get("grouping_columns", []):
                rules_dict[col_info["column_name"]] = col_info["grouping_logic"]
            if rules_dict:
                sample_df = apply_grouping_rules(sample_df, rules_dict)
                grouping_rules_applied = True
                print("✅ 分组规则应用完成")
        else:
            print("⚠️ 未检测到有效JSON规则")
    except Exception as e:
        print(f"⚠️ JSON解析失败: {e}")

    # Step 5️⃣ 写 Excel
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        original_df.to_excel(writer, sheet_name="metadata", index=False)
        study_df.to_excel(writer, sheet_name="study", index=False)
        sample_df.to_excel(writer, sheet_name="sample", index=False)
        if grouping_rules_applied and rules_json:
            rules_summary = [{
                "规则列": c["column_name"],
                "分组逻辑": str(c["grouping_logic"]),
                "置信度": c["confidence"],
                "说明": c["reason"]
            } for c in rules_json["grouping_columns"]]
            pd.DataFrame(rules_summary).to_excel(writer, sheet_name="grouping_rules", index=False)

    print(f"\n✅ 已生成 Excel 文件：{output_file}")
    print("📑 包含以下工作表: metadata, study, sample, grouping_rules")


if __name__ == "__main__":
    main()
