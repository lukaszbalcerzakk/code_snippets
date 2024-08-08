library(readr)
library(tidyverse)
library(stringdist)
df_file <- read_csv("C:/Users/lukasz.balcerzak/Desktop/R/code_snippets/tables_columns20240807.csv")

df= df_file |>
  mutate(
    tbl_name = if_else(str_starts(tbl,"shm"),str_remove(tbl,"shm"),tbl), # remove SHM prefix
    tbl_singular= str_remove(tbl_name,"e?s$"),  # make singular
    pk = tolower(col)==paste0(tbl_singular,"id"),  # compare names
    dist= diag(adist(col,paste0(tbl_singular,"id"),ignore.case = T))  # find most similar 
    )

# filter PKs
df_pk = df|>
  group_by(tbl)|>
  filter(dist==min(dist))|>
  ungroup() |>
  select(col,tbl)
  
df_fk = df |>
  select(col,tbl)|>
  anti_join(df_pk)


df_manual = tribble(
  ~tbl_pk,~col,~tbl_fk,
  "addresses","ObjectId","customers",
  "contacts","ObjectId","customers"
)

df_remove = 
  tribble(
    ~tbl_pk,~col,~tbl_fk,
    "addresses","AddressId","orders",
    "contacts","ContactId","orders",
    "addresses","AddressId","contacts",
    "customers","CustomerId","shmvisitresult"
  )

df_links = 
  left_join(df_pk,df_fk,by="col",suffix = c("_pk","_fk")) |>
  filter(!is.na(tbl_fk)) |>
  relocate(tbl_pk,.before=1)|>
  bind_rows(df_manual)|>  # add manual keys
  #anti_join(df_remove) |> # remove keys
  mutate(code=paste0(tbl_fk,"--",col,"-->",tbl_pk))|>
  arrange(tbl_fk,tbl_pk)


writeClipboard(c("flowchart LR",df_links$code))
