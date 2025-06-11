SELECT a.ID                              AS _id,
       a.ID                              AS id,
       a.DATASTATE                       AS operation,
       a.TRAINID                         AS trainingId,
       a.TRAINNAME                       AS training_name,
       -- ...其他字段...
       o.msstype                         AS org_class
FROM NU_TRAINSOURCEDATA_xzs_hyk a
         LEFT JOIN MC_ORG_SHOW b ON a.supDeptCode = b.id
         LEFT JOIN MC_ORG_SHOW o ON a.ORGANIZERID = o.id
         LEFT JOIN mc_org_show tao on tao.id = a.TRAINASSISTORGANIZER
WHERE a.hitdate = ?
LIMIT 1
