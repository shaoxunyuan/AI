#!/usr/bin/env Rscript
suppressMessages({
  library(rentrez)
  library(xml2)
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("Usage: Rscript bioproject_extract.R <PRJNA_ID>")
prjna <- args[1]

bioproject_xml <- entrez_fetch(db = "bioproject", id = prjna, rettype = "xml")
doc <- read_xml(bioproject_xml)

get_txt <- function(xpath) {
  node <- xml_find_first(doc, xpath)
  if (is.na(node) || is.null(node)) return(NA_character_)
  xml_text(node)
}
get_attr <- function(xpath) {
  node <- xml_find_first(doc, xpath)
  if (is.na(node) || is.null(node)) return(NA_character_)
  xml_attr(node, "accession")
}
get_attr_generic <- function(xpath, attr) {
  node <- xml_find_first(doc, xpath)
  if (is.na(node) || is.null(node)) return(NA_character_)
  xml_attr(node, attr)
}

project_accession <- get_attr("//Project/ProjectID/ArchiveID[@accession]")
geo_accession     <- get_txt("//Project/ProjectID/CenterID")
title             <- get_txt("//Project/ProjectDescr/Title")
description       <- get_txt("//Project/ProjectDescr/Description")
publication_id    <- get_attr_generic("//Project/ProjectDescr/Publication[@id]", "id")
publication_date  <- get_attr_generic("//Project/ProjectDescr/Publication[@date]", "date")
organism_name     <- get_txt("//Project/ProjectType/ProjectTypeSubmission/Target/Organism/OrganismName")

out <- list(
  project_accession = project_accession,
  geo_accession = geo_accession,
  title = title,
  description = description,
  publication_id = publication_id,
  #publication_date = publication_date,
  organism_name = organism_name
)
cat(toJSON(out, auto_unbox = TRUE, null = "null"))
#print(out)
