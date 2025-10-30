# 整合脚本：从BioProject提取元数据并获取关联GEO的PubMed摘要
# 功能：通过命令行传入BioProject编号，自动提取元数据及对应文献摘要

# ===================== 第一步：检查命令行参数（最优先）=====================
args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  cat("用法：Rscript 脚本名.R <BioProject编号>\n")
  cat("示例：Rscript bio_project_geo_abstract.R PRJNA979185\n")
  quit(status = 0)
}

prjnaid <- args[1]


# ===================== 第二步：静默加载所需R包（屏蔽所有启动信息）=====================
# 用suppressPackageStartupMessages屏蔽所有包的加载提示、冲突警告、欢迎语等
suppressPackageStartupMessages({
  library(rentrez)
  library(xml2)
  library(dplyr)
  library(GEOquery)
  
  # 关闭GEOquery的配置信息输出
  options(GEOquery.verbose = FALSE)
})


# ===================== 第三步：从BioProject提取元数据 =====================
bioproject_xml <- entrez_fetch(
  db = "bioproject",
  id = prjnaid,
  rettype = "xml"
)

doc <- read_xml(bioproject_xml)

fields <- list(
  project_accession = "//Project/ProjectID/ArchiveID@accession",
  geo_accession     = "//Project/ProjectDescr/ExternalLink/dbXREF/ID",
  title             = "//Project/ProjectDescr/Title",
  description       = "//Project/ProjectDescr/Description",
  pmid              = "//Project/ProjectDescr/Publication/Reference",
  species           = "//Project/ProjectType/ProjectTypeSubmission/Target/Organism/OrganismName",
  submission_date   = "//Submission@submitted"
)

result <- list()
for (field_name in names(fields)) {
  path <- fields[[field_name]]
  
  if (grepl("@", path)) {
    node_path <- sub("@.*$", "", path)
    attr_name <- sub(".*@", "", path)
    node <- xml_find_first(doc, node_path)
    value <- ifelse(is.na(node), "", xml_attr(node, attr_name))
  } else {
    node <- xml_find_first(doc, path)
    value <- ifelse(is.na(node), "", xml_text(node))
  }
  
  result[[field_name]] <- value
}


# ===================== 第四步：通过GEO编号获取关联文献摘要 =====================
geo_id <- result$geo_accession

# 抑制getGEO的所有运行时消息
gse <- suppressMessages(
  getGEO(geo_id, GSEMatrix = FALSE)
)

pubmed_id <- Meta(gse)$pubmed_id
abstract <- entrez_fetch(db = "pubmed", id = pubmed_id, rettype = "abstract")
abstract <- gsub("\n", "--", abstract)


# ===================== 输出结果 =====================
cat("=== BioProject元数据 ===\n")
result
cat("\n=== 关联文献摘要 ===\n")
abstract