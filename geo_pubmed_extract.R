#!/usr/bin/env Rscript
suppressMessages({
  library(GEOquery)
  library(rentrez)
  library(jsonlite)
})

#===============================
# 函数：根据 PubMed ID 抓取详细信息并格式化成 list
#===============================
fetch_pubmed_as_list <- function(pubmed_id) {
  raw_txt <- tryCatch({
    entrez_fetch(db = "pubmed", id = pubmed_id, rettype = "medline", retmode = "text")
  }, error = function(e) NA)

  if (is.na(raw_txt) || nchar(raw_txt) == 0) {
    return(list(
      pubmed_id = pubmed_id,
      title = NA,
      abstract = NA,
      journal = NA,
      pub_date = NA,
      doi = NA
    ))
  }

  lines <- strsplit(raw_txt, "\n")[[1]]

  result <- list(
    pubmed_id = pubmed_id,
    title = "",
    abstract = "",
    journal = "",
    pub_date = "",
    doi = ""
  )

  current_field <- NULL

  for (ln in lines) {
    if (grepl("^TI  -", ln)) {
      result$title <- sub("^TI  -\\s*", "", ln)
      current_field <- "title"
    } else if (grepl("^AB  -", ln)) {
      result$abstract <- paste(result$abstract, sub("^AB  -\\s*", "", ln))
      current_field <- "abstract"
    } else if (grepl("^JT  -", ln)) {
      result$journal <- sub("^JT  -\\s*", "", ln)
      current_field <- NULL
    } else if (grepl("^DP  -", ln)) {
      result$pub_date <- sub("^DP  -\\s*", "", ln)
      current_field <- NULL
    } else if (grepl("^LID - .* \\[doi\\]", ln)) {
      result$doi <- sub("^LID - (.*) \\[doi\\].*", "\\1", ln)
      current_field <- NULL
    } else if (grepl("^\\s{6}", ln) && !is.null(current_field)) {
      result[[current_field]] <- paste(result[[current_field]], trimws(ln))
    }
  }

  result <- lapply(result, function(x) if (is.character(x)) trimws(x) else x)
  return(result)
}

#===============================
# 主流程：根据 GSE ID 获取 PubMed 信息
#===============================
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("Usage: Rscript geo_pubmed_extract.R <GSE_ID>")
geo_id <- args[1]

if (is.na(geo_id) || is.null(geo_id) || nchar(geo_id) == 0) {
  print(list(
    pubmed_id = NA,
    title = NA,
    abstract = NA,
    journal = NA,
    pub_date = NA,
    doi = NA
  ))
  quit(save = "no")
}

#===============================
# 获取 GEO 信息
#===============================
gse <- suppressMessages(getGEO(geo_id, GSEMatrix = FALSE))
pmid_list <- tryCatch({ Meta(gse)$pubmed_id }, error = function(e) NA)

# 如果没有 PubMed ID，则用标题搜索
if (is.null(pmid_list) || length(pmid_list) == 0) {
  title <- tryCatch({ Meta(gse)$title }, error = function(e) NA)
  if (!is.na(title) && nchar(title) > 0) {
    srch <- entrez_search(db = "pubmed", term = paste0(title, "[Title]"), retmax = 1)
    if (length(srch$ids) > 0) pmid_list <- srch$ids
  }
}

#===============================
# 提取多个 PubMed 信息
#===============================
pub_list <- list()
if (!is.null(pmid_list) && length(pmid_list) > 0) {
  for (id in pmid_list) {
    info <- fetch_pubmed_as_list(id)
    pub_list[[id]] <- info
  }
} else {
  pub_list[["NA"]] <- list(
    pubmed_id = NA,
    title = NA,
    abstract = NA,
    journal = NA,
    pub_date = NA,
    doi = NA
  )
}

#===============================
# 输出 JSON 结构（多个 PubMed）
#===============================
cat(toJSON(pub_list, auto_unbox = TRUE, null = "null"))
