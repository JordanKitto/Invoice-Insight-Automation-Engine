SELECT *
FROM DSS.VIM_1HEAD_2HEAD_VW h
JOIN DSS.VIM_STG_T101T_VW s
  ON h.status = s.statusid
LEFT JOIN DSS.VIM_OTX_PF01_T_1REG_AP_VW o
  ON h.docid = o.target_projkey
WHERE h.index_date >= TO_DATE ('2023-07-01', 'YYYY-MM-DD')

