{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}

WITH sawith0 AS (
    SELECT
        t5530353.c24089407  AS c1,
        t5530353.c8982338   AS c2,
        t5530353.c53381413  AS c3,
        t5530353.c394379981 AS c4,
        t5530353.c477462940 AS c5,
        t5530353.c108670069 AS c6,
        t5530353.c251365184 AS c7,
        t5530353.c292741628 AS c8,
        t5530353.c420730390 AS c9,
        t5530353.c455945806 AS c10,
        t5530353.c194891982 AS c11,
        t5530353.c203809729 AS c12,
        t5530353.c348575859 AS c13,
        t5530353.c501091705 AS c14,
        t5530353.c105438889 AS c15,
        t5530353.c181846264 AS c16,
        t5530353.c300265324 AS c17,
        t5530353.c193267114 AS c18,
        t5530353.c57387448  AS c19,
        t5530353.c479053863 AS c20,
        t5530353.c308483835 AS c21,
        t5530353.c527367620 AS c22,   
        t5530353.c414419258 AS c61,
        t5530353.c214988020 AS c62,
        t5530353.c171806690 AS c63,
        t5530353.c467188093 AS c64,
        t5530353.c477098923 AS c66,
        t5530353.c16394543  AS c67
    FROM
        (
            SELECT
                v451654944.dep0_value           AS c24089407,
                v451654944.dep0_description     AS c8982338,
                v451654944.dep1_pk1_value       AS c53381413,
                v451654944.dep1_description     AS c394379981,
                v451654944.dep2_pk1_value       AS c477462940,
                v451654944.dep2_description     AS c108670069,
                v451654944.dep3_pk1_value       AS c251365184,
                v451654944.dep3_description     AS c292741628,
                v451654944.dep4_pk1_value       AS c420730390,
                v451654944.dep4_description     AS c455945806,
                v451654944.dep5_pk1_value       AS c194891982,
                v451654944.dep5_description     AS c203809729,
                v451654944.dep6_pk1_value       AS c348575859,
                v451654944.dep6_description     AS c501091705,
                v451654944.dep7_pk1_value       AS c105438889,
                v451654944.dep7_description     AS c181846264,
                v451654944.dep8_pk1_value       AS c300265324,
                v451654944.dep8_description     AS c193267114,
                v451654944.dep9_pk1_value       AS c57387448,
                v451654944.dep9_description     AS c479053863,
                v451654944.dep10_pk1_value      AS c308483835,
                v451654944.dep10_description    AS c527367620,    
                v451654944.dep30_pk1_value      AS c414419258,
                v451654944.dep30_description    AS c214988020,
                v451654944.dep31_pk1_value      AS c171806690,
                v451654944.dep31_description    AS c467188093,
                v181200083.tree_name            AS c477098923,
                v181200083.tree_version_name    AS c16394543,
                v451654944.enterprise_id        AS pka_enterpriseid0,
                v451654944.tree_structure_code  AS pka_treestructurecode0,
                v451654944.tree_code            AS pka_treecode0,
                v451654944.tree_version_id      AS pka_treeversionid0,
                v451654944.cf_tree_node_id      AS pka_cftreenodeid0,
                v451654944.dep0_value_id        AS pka_dep0valueid0,
                v451654944.dep1_value_id        AS pka_dep1valueid0,
                v451654944.dep2_value_id        AS pka_dep2valueid0,
                v451654944.dep3_value_id        AS pka_dep3valueid0,
                v451654944.dep4_value_id        AS pka_dep4valueid0,
                v451654944.dep5_value_id        AS pka_dep5valueid0,
                v451654944.dep6_value_id        AS pka_dep6valueid0,
                v451654944.dep7_value_id        AS pka_dep7valueid0,
                v451654944.dep8_value_id        AS pka_dep8valueid0,
                v451654944.dep9_value_id        AS pka_dep9valueid0,
                v451654944.dep10_value_id       AS pka_dep10valueid0,           
                v451654944.dep30_value_id       AS pka_dep30valueid0,
                v451654944.dep31_value_id       AS pka_dep31valueid0,
                v181200083.tree_structure_code  AS pka_treestructurecode1,
                v181200083.tree_code            AS pka_treecode1,
                v181200083.tree_structure_code1 AS pka_versiontreestructurecode0,
                v181200083.tree_code1           AS pka_versiontreecode0,
                v181200083.tree_version_id      AS pka_treeversionid1,
                v181200083.effective_start_date AS pka_versioneffectivestartdate0,
                v181200083.effective_end_date   AS pka_versioneffectiveenddate0
            FROM
                (
                    SELECT 
                        cft.ENTERPRISE_ID as enterprise_id,
                        cft.TREE_STRUCTURE_CODE as tree_structure_code,
                        cft.TREE_CODE as tree_code,
                        cft.TREE_VERSION_ID as tree_version_id,
                        cft.CF_TREE_NODE_ID as cf_tree_node_id,
                        cft.DEP_1_PK_1_VALUE as dep1_pk1_value,
                        cft.DEP_2_PK_1_VALUE as dep2_pk1_value,
                        cft.DEP_3_PK_1_VALUE as dep3_pk1_value,
                        cft.DEP_4_PK_1_VALUE as dep4_pk1_value,
                        cft.DEP_5_PK_1_VALUE as dep5_pk1_value,
                        cft.DEP_6_PK_1_VALUE as dep6_pk1_value,
                        cft.DEP_7_PK_1_VALUE as dep7_pk1_value,
                        cft.DEP_8_PK_1_VALUE as dep8_pk1_value,
                        cft.DEP_9_PK_1_VALUE as dep9_pk1_value,
                        cft.DEP_10_PK_1_VALUE as dep10_pk1_value,
                        cft.DEP_11_PK_1_VALUE as dep11_pk1_value,
                        cft.DEP_12_PK_1_VALUE as dep12_pk1_value,
                        cft.DEP_13_PK_1_VALUE as dep13_pk1_value,
                        cft.DEP_14_PK_1_VALUE as dep14_pk1_value,
                        cft.DEP_15_PK_1_VALUE as dep15_pk1_value,
                        cft.DEP_16_PK_1_VALUE as dep16_pk1_value,
                        cft.DEP_17_PK_1_VALUE as dep17_pk1_value,
                        cft.DEP_18_PK_1_VALUE as dep18_pk1_value,
                        cft.DEP_19_PK_1_VALUE as dep19_pk1_value,
                        cft.DEP_20_PK_1_VALUE as dep20_pk1_value,
                        cft.DEP_21_PK_1_VALUE as dep21_pk1_value,
                        cft.DEP_22_PK_1_VALUE as dep22_pk1_value,
                        cft.DEP_23_PK_1_VALUE as dep23_pk1_value,
                        cft.DEP_24_PK_1_VALUE as dep24_pk1_value,
                        cft.DEP_25_PK_1_VALUE as dep25_pk1_value,
                        cft.DEP_26_PK_1_VALUE as dep26_pk1_value,
                        cft.DEP_27_PK_1_VALUE as dep27_pk1_value,
                        cft.DEP_28_PK_1_VALUE as dep28_pk1_value,
                        cft.DEP_29_PK_1_VALUE as dep29_pk1_value,
                        cft.DEP_30_PK_1_VALUE as dep30_pk1_value,
                        cft.DEP_31_PK_1_VALUE as dep31_pk1_value,
                        dsdep0_1.value_id     AS dep0_value_id,
                        dsdep0_1.value        AS dep0_value,
                        dsdep0_1.description  AS dep0_description,
                        dsdep1_1.value_id     AS dep1_value_id,
                        dsdep1_1.description  AS dep1_description,
                        dsdep2_1.value_id     AS dep2_value_id,
                        dsdep2_1.description  AS dep2_description,
                        dsdep3_1.value_id     AS dep3_value_id,
                        dsdep3_1.description  AS dep3_description,
                        dsdep4_1.value_id     AS dep4_value_id,
                        dsdep4_1.description  AS dep4_description,
                        dsdep5_1.value_id     AS dep5_value_id,
                        dsdep5_1.description  AS dep5_description,
                        dsdep6_1.value_id     AS dep6_value_id,
                        dsdep6_1.description  AS dep6_description,
                        dsdep7_1.value_id     AS dep7_value_id,
                        dsdep7_1.description  AS dep7_description,
                        dsdep8_1.value_id     AS dep8_value_id,
                        dsdep8_1.description  AS dep8_description,
                        dsdep9_1.value_id     AS dep9_value_id,
                        dsdep9_1.description  AS dep9_description,
                        dsdep10_1.value_id    AS dep10_value_id,
                        dsdep10_1.description AS dep10_description,          
                        dsdep30_1.value_id    AS dep30_value_id,
                        dsdep30_1.description AS dep30_description,
                        dsdep31_1.value_id    AS dep31_value_id,
                        dsdep31_1.description AS dep31_description

                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.SEGMENT_VALUE_HIERARCHY_EXTRACT_PVO     cft, 
                        
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep0_1,
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep1_1,
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep2_1,
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep3_1,    
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep4_1,  
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep5_1,  
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep6_1,  
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep7_1,  
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep8_1,  
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep9_1,  
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep10_1, 
 
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep30_1, 
                  
                  
                        ( 
                        select 
                        V.VALUE_ID_1     AS value_id,
                        V.VALUE        AS value,
                        V.DESCRIPTION  AS description,
                        VS.VALUE_SET_CODE as value_set_code
                        FROM 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_VALUES_PVO  V,  --FND_VS_VALUES_VL 
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.VALUE_SET_TYPED_VALUES_PVO VS --FND_VS_VALUE_SETS 
                        WHERE V.VALUE_ID = VS.VALUE_ID
                        )  dsdep31_1 
                           
                    WHERE
                          cft.DEP_0_PK_1_VALUE = dsdep0_1.VALUE (+)
                          AND cft.DEP_0_PK_2_VALUE = dsdep0_1.VALUE_SET_CODE (+)
                          AND cft.DEP_1_PK_1_VALUE  = dsdep1_1.VALUE (+)
                          AND cft.DEP_1_PK_2_VALUE  = dsdep1_1.VALUE_SET_CODE (+)
                          AND cft.DEP_2_PK_1_VALUE  = dsdep2_1.VALUE (+)
                          AND cft.DEP_2_PK_2_VALUE  = dsdep2_1.VALUE_SET_CODE (+)                   
                          AND cft.DEP_3_PK_1_VALUE  = dsdep3_1.VALUE (+)
                          AND cft.DEP_3_PK_2_VALUE  = dsdep3_1.VALUE_SET_CODE (+)
                          AND cft.DEP_4_PK_1_VALUE  = dsdep4_1.VALUE (+)
                          AND cft.DEP_4_PK_2_VALUE  = dsdep4_1.VALUE_SET_CODE (+)
                          AND cft.DEP_5_PK_1_VALUE  = dsdep5_1.VALUE (+)
                          AND cft.DEP_5_PK_2_VALUE  = dsdep5_1.VALUE_SET_CODE (+)
                          AND cft.DEP_6_PK_1_VALUE  = dsdep6_1.VALUE (+)
                          AND cft.DEP_6_PK_2_VALUE  = dsdep6_1.VALUE_SET_CODE (+)
                          AND cft.DEP_7_PK_1_VALUE  = dsdep7_1.VALUE (+)
                          AND cft.DEP_7_PK_2_VALUE  = dsdep7_1.VALUE_SET_CODE (+)
                          AND cft.DEP_8_PK_1_VALUE  = dsdep8_1.VALUE (+)
                          AND cft.DEP_8_PK_2_VALUE  = dsdep8_1.VALUE_SET_CODE (+)
                          AND cft.DEP_9_PK_1_VALUE = dsdep9_1.VALUE (+)
                          AND cft.DEP_9_PK_2_VALUE = dsdep9_1.VALUE_SET_CODE (+)
                          AND cft.DEP_10_PK_1_VALUE = dsdep10_1.VALUE (+)
                          AND cft.DEP_10_PK_2_VALUE = dsdep10_1.VALUE_SET_CODE (+)
                          AND cft.DEP_30_PK_1_VALUE = dsdep30_1.VALUE (+)
                          AND cft.DEP_30_PK_2_VALUE = dsdep30_1.VALUE_SET_CODE (+)
                          AND cft.DEP_31_PK_1_VALUE = dsdep31_1.VALUE (+)
                          AND cft.DEP_31_PK_2_VALUE = dsdep31_1.VALUE_SET_CODE (+)
                          AND cft.TREE_CODE LIKE 'Scheme%'
                          /*
                          IN (  --This can be changed depending on the required dimension, e.g. gl account, cc etc
                             'Account Scheme Ledger'
                            ,'Account Scheme TAS Ledger'
                            ,'Account Scheme VIC NOP Ledger'
                            ,'Account Service Co Ledger'
                            ,'Account VIC Scheme Ledger'
                            ,'Account WARRRL WA Ledger'
                            )
                          */
                ) v451654944,
                (
                    SELECT
                        fndtree.TREE_STRUCTURE_CODE as tree_structure_code,
                        fndtree.TREE_CODE as tree_code,
                        fndtree.TREE_NAME as tree_name,
                        fndtree.VERSION_TREE_STRUCTURE_CODE AS tree_structure_code1,
                        fndtree.VERSION_TREE_CODE           AS tree_code1,
                        fndtree.TREE_VERSION_ID as tree_version_id,
                        fndtree.VERSION_EFFECTIVE_START_DATE as effective_start_date,
                        fndtree.VERSION_EFFECTIVE_END_DATE as effective_end_date,
                        fndtree.VERSION_NAME as tree_version_name
                    FROM
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.FND_TREE_AND_VERSION_VO fndtree
                    WHERE
                         SYSDATE() BETWEEN fndtree.VERSION_EFFECTIVE_START_DATE AND fndtree.VERSION_EFFECTIVE_END_DATE 
                        AND   fndtree.TREE_STRUCTURE_CODE = 'GL_ACCT_FLEX'
                        AND fndtree.TREE_CODE LIKE 'Scheme%'
                        /*
                        IN (  
                             'Account Scheme Ledger'
                            ,'Account Scheme TAS Ledger'
                            ,'Account Scheme VIC NOP Ledger'
                            ,'Account Service Co Ledger'
                            ,'Account VIC Scheme Ledger'
                            ,'Account WARRRL WA Ledger'
                            )
                         */
   
                ) v181200083
            WHERE
                    v451654944.tree_structure_code = v181200083.tree_structure_code (+)
                AND v451654944.tree_version_id = v181200083.tree_version_id (+)
                AND v451654944.tree_code = v181200083.tree_code (+)
        ) t5530353
)
SELECT DISTINCT
    d1.c1                               AS SCHEME_CODE,
    d1.c2                               AS SCHEME_DESC,
    d1.c3                               AS SCHEME_L1_CODE,
    d1.c4                               AS SCHEME_L1_DESC,
    d1.c5                               AS SCHEME_L2_CODE,
    d1.c6                               AS SCHEME_L2_DESC,
    d1.c7                               AS SCHEME_L3_CODE,
    d1.c8                               AS SCHEME_L3_DESC,
    d1.c9                               AS SCHEME_L4_CODE,
    d1.c10                              AS SCHEME_L4_DESC,
    d1.c11                              AS SCHEME_L5_CODE,
    d1.c12                              AS SCHEME_L5_DESC,
    d1.c13                              AS SCHEME_L6_CODE,
    d1.c14                              AS SCHEME_L6_DESC,
    d1.c15                              AS SCHEME_L7_CODE,
    d1.c16                              AS SCHEME_L7_DESC,
    d1.c17                              AS SCHEME_L8_CODE,
    d1.c18                              AS SCHEME_L8_DESC,
    d1.c19                              AS SCHEME_L9_CODE,
    d1.c20                              AS SCHEME_L9_DESC,
    d1.c21                              AS SCHEME_L10_CODE,
    d1.c22                              AS SCHEME_L10_DESC,
    d1.c61                              AS SCHEME_L30_CODE,
    d1.c62                              AS SCHEME_L30_DESC,
    d1.c63                              AS SCHEME_L31_CODE,
    d1.c64                              AS SCHEME_L31_DESC,
    concat(concat(d1.c66, '-'), d1.c67) AS SCHEME_TREE_CODE
FROM
    sawith0 d1
WHERE d1.c67 IS NOT NULL 
ORDER BY SCHEME_CODE, SCHEME_TREE_CODE
