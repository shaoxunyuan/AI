#!/usr/bin/env python3
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
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system",
             "content": "你是一名资深生物信息专家，擅长解析 SRA metadata(pysradb) 并提取样本分组逻辑。"},
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


# ========== 使用pysradb获取metadata ==========
def get_metadata_with_pysradb(prj_id):
    """
    使用pysradb命令行工具获取metadata
    """
    print(f"📥 正在下载 {prj_id} 的metadata...")
    
    try:
        # 执行pysradb命令
        cmd = ["pysradb", "metadata", prj_id, "--expand"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        if result.returncode != 0:
            print(f"❌ pysradb命令执行失败: {result.stderr}")
            return None
            
        # 解析输出的TSV数据
        metadata_df = pd.read_csv(StringIO(result.stdout), sep="\t", dtype=str)
        print(f"✅ 成功获取 {len(metadata_df)} 条样本记录")
        return metadata_df
        
    except subprocess.CalledProcessError as e:
        print(f"❌ pysradb命令执行错误: {e}")
        print(f"stderr: {e.stderr}")
        return None
    except Exception as e:
        print(f"❌ 解析metadata时出错: {e}")
        return None


# ========== 批量规则应用函数 ==========
def apply_grouping_rules(sample_df, rules_dict):
    """
    根据规则字典批量应用分组逻辑
    rules_dict: {列名: 规则字典}
    规则字典格式: {字段值: 分组名称}
    """
    sample_df = sample_df.copy()
    
    # 为每个规则列创建分组列
    for col_name, rules in rules_dict.items():
        if col_name not in sample_df.columns:
            print(f"⚠️  警告: 列 '{col_name}' 不存在，跳过该规则")
            continue
            
        group_col_name = f"group_{col_name}"
        sample_df[group_col_name] = "NA"  # 默认值
        
        # 应用规则
        for value_pattern, group_name in rules.items():
            # 处理正则表达式模式
            if value_pattern.startswith("regex:"):
                pattern = value_pattern[6:]
                mask = sample_df[col_name].str.contains(pattern, case=False, na=False)
                sample_df.loc[mask, group_col_name] = group_name
            # 处理精确匹配
            else:
                mask = sample_df[col_name] == value_pattern
                sample_df.loc[mask, group_col_name] = group_name
    
    return sample_df


# ========== 主流程 ==========
def main():
    if len(sys.argv) < 2:
        print("用法: python parse_metadata_to_excel.py PRJNAxxxxxx")
        print("示例: python parse_metadata_to_excel.py PRJNA123456")
        sys.exit(1)

    prj_id = sys.argv[1]
    output_file = f"{prj_id}_metadata.xlsx"

    # 检查pysradb是否可用
    try:
        subprocess.run(["pysradb", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ 未找到pysradb，请先安装: pip install pysradb")
        sys.exit(1)

    # Step 1️⃣ 使用pysradb获取metadata
    df = get_metadata_with_pysradb(prj_id)
    if df is None:
        print("❌ 无法获取metadata，请检查PRJNA ID是否正确")
        sys.exit(1)
        
    original_df = df.copy()  # 保存原始数据
    df = df.fillna("NA")

    print(f"✅ 已读取 metadata：{len(df)} 行 × {len(df.columns)} 列")
    print("📊 列信息:")
    for col in df.columns:
        unique_count = df[col].nunique()
        print(f"   - {col}: {unique_count} 个唯一值")
        if unique_count <= 10 and unique_count > 1:  # 只显示较少唯一值的具体内容
            print(f"     唯一值: {list(df[col].unique())}")

    # Step 2️⃣ 自动判断 study-level 与 sample-level 列
    study_cols = []
    sample_cols = []
    for col in df.columns:
        if df[col].nunique() == 1:
            study_cols.append(col)
        else:
            sample_cols.append(col)

    study_df = pd.DataFrame({
        "字段名": study_cols,
        "字段值": [df[col].iloc[0] for col in study_cols]
    })
    sample_df = df[sample_cols].copy()

    print(f"\n🧩 Study 层字段: {len(study_cols)} 个")
    print(f"🧬 Sample 层字段: {len(sample_cols)} 个")

    # Step 3️⃣ 构造 AI 提示词 - 只让AI理解逻辑，不生成代码
    preview_data = []
    for col in sample_cols:
        unique_vals = sample_df[col].unique()[:10]  # 每列最多显示10个唯一值
        preview_data.append({
            "列名": col,
            "唯一值数量": sample_df[col].nunique(),
            "示例值": list(unique_vals)
        })
    
    preview_df = pd.DataFrame(preview_data)
    
    prompt = f"""
作为生物信息专家，请分析以下SRA metadata样本数据的分组逻辑：

数据集列信息：
{preview_df.to_string(index=False)}

请基于这些列的数据模式，识别出可能的分组逻辑。重点关注：
1. 哪些列包含明显的分组信息（如AS_patient, Healthy等）
2. 这些列中的值如何对应到不同的实验组

当前数据中关键列的所有唯一值：
experiment_title列所有值：{list(sample_df['experiment_title'].unique())}

请用以下JSON格式输出分析结果，只输出JSON，不要其他说明：
{{
    "grouping_columns": [
        {{
            "column_name": "列名",
            "grouping_logic": {{
                "字段值或模式1": "分组名称1",
                "字段值或模式2": "分组名称2"
            }},
            "confidence": "高/中/低",
            "reason": "分组逻辑说明"
        }}
    ]
}}

重要要求：
- 对于文本模式匹配，使用 "regex:模式" 作为键
- 分组名称要简洁明确（如"AS", "Control"）
- 只输出最有把握的分组规则
- 必须确保分组规则能够覆盖数据中所有出现的模式，不能有遗漏
- 仔细分析experiment_title列的所有唯一值，确保每个值都能匹配到某个规则
- 如果发现数据中有多种明显不同的模式，都应该包含在分组逻辑中
- 请确保分组规则的顺序是从一般到特殊，即更一般的规则（匹配范围大）在前，更特殊的规则（匹配范围小）在后。例如，先匹配“.*disease.*”，再匹配“.*severe disease.*”
"""

    print("🤖 正在让 DeepSeek 分析样本分组逻辑...")
    analysis_result = ask_deepseek(prompt)
    print("\n=== DeepSeek 分析结果 ===\n")
    print(analysis_result)
    print("========================\n")

    # Step 4️⃣ 解析AI返回的分组规则并应用
    grouping_rules_applied = False
    rules_json = None
    try:
        # 提取JSON部分（处理可能的格式问题）
        json_match = re.search(r'\{.*\}', analysis_result, re.DOTALL)
        if json_match:
            rules_json = json.loads(json_match.group())
            
            rules_dict = {}
            for col_info in rules_json.get("grouping_columns", []):
                col_name = col_info["column_name"]
                grouping_logic = col_info["grouping_logic"]
                confidence = col_info["confidence"]
                reason = col_info["reason"]
                
                print(f"📋 应用分组规则 - 列: {col_name}")
                print(f"   置信度: {confidence}")
                print(f"   理由: {reason}")
                print(f"   规则: {grouping_logic}")
                
                rules_dict[col_name] = grouping_logic
            
            # 应用分组规则
            if rules_dict:
                sample_df = apply_grouping_rules(sample_df, rules_dict)
                grouping_rules_applied = True
                print("✅ 分组规则应用完成")
            else:
                print("⚠️  未找到有效的分组规则")
            
        else:
            print("❌ 无法解析DeepSeek返回的分组规则")
            
    except Exception as e:
        print(f"❌ 解析分组规则时出错: {e}")
        print("⚠️  将继续输出未分组的metadata")

    # Step 5️⃣ 输出 Excel 文件（包含三个sheet）
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # Sheet 1: 原始metadata
        original_df.to_excel(writer, sheet_name="metadata", index=False)
        
        # Sheet 2: study层面信息
        study_df.to_excel(writer, sheet_name="study", index=False)
        
        # Sheet 3: sample层面信息（包含分组结果）
        sample_df.to_excel(writer, sheet_name="sample", index=False)
        
        # 可选：添加一个分组规则说明的sheet
        if grouping_rules_applied and rules_json:
            rules_summary = []
            for col_info in rules_json.get("grouping_columns", []):
                rules_summary.append({
                    "规则列": col_info["column_name"],
                    "分组逻辑": str(col_info["grouping_logic"]),
                    "置信度": col_info["confidence"],
                    "说明": col_info["reason"]
                })
            rules_df = pd.DataFrame(rules_summary)
            rules_df.to_excel(writer, sheet_name="grouping_rules", index=False)

    print(f"\n✅ 已生成 Excel 文件：{output_file}")
    print("📑 包含以下工作表:")
    print(f"   - metadata: {len(original_df)} 行原始数据")
    print(f"   - study: {len(study_df)} 条项目字段")
    print(f"   - sample: {len(sample_df)} 条样本记录")
    if grouping_rules_applied:
        print(f"   - grouping_rules: 分组规则说明")
    
    # 显示分组结果统计
    if grouping_rules_applied:
        group_cols = [col for col in sample_df.columns if col.startswith('group_')]
        if group_cols:
            print("\n📈 分组结果统计:")
            for col in group_cols:
                print(f"   {col}:")
                group_counts = sample_df[col].value_counts()
                for group, count in group_counts.items():
                    print(f"     - {group}: {count} 个样本")


if __name__ == "__main__":
    main()