def load_sql():
    """Returns a dictionnary of SQL queries"""
    
    sql = {}
    
    sql['inactive_lands'] = """
                SELECT
                      CAST(IP.INTRID_SID AS NUMBER) INTRID_SID,
                      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_SID,
                      DS.FILE_CHR,
                      SG.STAGE_NME,
                      TT.ACTIVATION_CDE AS DTS_ACTIVATION_CDE,
                      TT.STATUS_NME,
                      TS.EFFECTIVE_DAT,
                      PU.PURPOSE_NME,
                      SP.SUBPURPOSE_NME,
                      TY.TYPE_NME,
                      ST.SUBTYPE_NME,
                      DT.LOCATION_DSC,
                      PR.LEGAL_NAME AS HOLDER_ORGANNSATION_NAME,
                      PR.FIRST_NAME || ' ' || PR.LAST_NAME AS HOLDER_INDIVIDUAL_NAME
                FROM WHSE_TANTALIS.TA_DISPOSITION_TRANSACTIONS DT 
                  JOIN WHSE_TANTALIS.TA_INTEREST_PARCELS IP 
                    ON DT.DISPOSITION_TRANSACTION_SID = IP.DISPOSITION_TRANSACTION_SID
                      AND IP.EXPIRY_DAT IS NULL
                  JOIN WHSE_TANTALIS.TA_DISP_TRANS_STATUSES TS
                    ON DT.DISPOSITION_TRANSACTION_SID = TS.DISPOSITION_TRANSACTION_SID 
                      AND TS.EXPIRY_DAT IS NULL
                  JOIN WHSE_TANTALIS.TA_DISPOSITIONS DS
                    ON DS.DISPOSITION_SID = DT.DISPOSITION_SID
                  JOIN WHSE_TANTALIS.TA_STAGES SG 
                    ON SG.CODE_CHR = TS.CODE_CHR_STAGE
                  JOIN WHSE_TANTALIS.TA_STATUS TT 
                    ON TT.CODE_CHR = TS.CODE_CHR_STATUS
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_TYPES TY 
                    ON TY.TYPE_SID = DT.TYPE_SID    
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBTYPES ST 
                    ON ST.SUBTYPE_SID = DT.SUBTYPE_SID 
                      AND ST.TYPE_SID = DT.TYPE_SID 
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_PURPOSES PU 
                    ON PU.PURPOSE_SID = DT.PURPOSE_SID    
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBPURPOSES SP 
                    ON SP.SUBPURPOSE_SID = DT.SUBPURPOSE_SID 
                      AND SP.PURPOSE_SID = DT.PURPOSE_SID 
                   JOIN WHSE_TANTALIS.TA_ORGANIZATION_UNITS OU 
                    ON OU.ORG_UNIT_SID = DT.ORG_UNIT_SID 
                   JOIN WHSE_TANTALIS.TA_TENANTS TE 
                    ON TE.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
                      AND TE.SEPARATION_DAT IS NULL
                     --AND TE.PRIMARY_CONTACT_YRN = 'Y'
                   JOIN WHSE_TANTALIS.TA_INTERESTED_PARTIES PR
                    ON PR.INTERESTED_PARTY_SID = TE.INTERESTED_PARTY_SID
                WHERE    
                      TT.ACTIVATION_CDE = 'INACT' AND  
                      SG.STAGE_NME = 'TENURE' AND
                      {prcl} AND 
                      DS.FILE_CHR NOT IN (SELECT TEN.CROWN_LANDS_FILE
                                           FROM WHSE_TANTALIS.TA_CROWN_TENURES_VW TEN
                                           WHERE TEN.TENURE_STATUS IN ('DISPOSITION IN GOOD STANDING', 'OFFERED', 'OFFER ACCEPTED'))
                ORDER BY TS.EFFECTIVE_DAT DESC
                    """
    
    
    sql['inactive_lands_wkb'] = """
                select
                      cast(mm.intrid_sid as number) intrid_sid,
                      cast(mm.disposition_transaction_sid as number) disposition_transaction_sid,
                      nn.file_chr,
                      nn.stage_nme,
                      nn.dts_activation_cde,
                      nn.status_nme,
                      --mm.expiry_dat,
                      nn.purpose_nme,
                      nn.subpurpose_nme,
                      nn.type_nme,
                      nn.subtype_nme,
                      nn.location_dsc,
                      nn.ORGANIZATIONS_LEGAL_NAME as holder_organnsation_name,
                      nn.INDIVIDUALS_FIRST_NAME || ' ' || nn.INDIVIDUALS_LAST_NAME as holder_individual_name
                      --nn.CITY as holder_city,
                      --nn.REGION_CDE as holder_region,
                      --nn.COUNTRY_CDE as holder_country,
                      --nn.WORK_AREA_CODE || nn.WORK_EXTENSION_NUMBER|| nn.WORK_PHONE_NUMBER as holder_phone      
              from (select
                   cast(jj.disposition_transaction_sid as number) disposition_transaction_sid,
                   cast(jj.disposition_sid as number) disposition_sid,
                   kk.activation_cde as d_activation_cde,
                   kk.file_chr,
                   jj.purpose_nme,
                   jj.subpurpose_nme,
                   jj.type_nme,
                   jj.subtype_nme,
                   jj.unit_name,
                   jj.street_address,
                   jj.province_abbr,
                   jj.commencement_dat,
                   jj.document_chr,
                   jj.entered_dat,
                   jj.expiry_dat as dt_expiry_dat,
                   jj.location_dsc,
                   jj.offer_timber_deferment_yrn,
                   jj.received_dat,
                   jj.surface_or_under_cde,
                   jj.timber_value_dlr,
                   ll.effective_dat,
                   ll.code_chr_status,
                   ll.status_nme,
                   ll.dts_activation_cde,
                   ll.status_expiry_dat,
                   ll.code_chr_stage,
                   ll.stage_nme,
                   ll.stage_expiry_dat,
                   jj.ORGANIZATIONS_LEGAL_NAME,
                   jj.INDIVIDUALS_FIRST_NAME,
                   jj.INDIVIDUALS_LAST_NAME,
                   jj.CITY,
                   jj.REGION_CDE,
                   jj.COUNTRY_CDE,
                   jj.WORK_AREA_CODE,
                   jj.WORK_EXTENSION_NUMBER,
                   jj.WORK_PHONE_NUMBER
                   
             from (select
                   cast(aa.disposition_transaction_sid as number) disposition_transaction_sid,
                   cast(aa.disposition_sid as number) disposition_sid,
                   bb.purpose_nme,
                   cc.subpurpose_nme,
                   dd.type_nme,
                   ee.subtype_nme,
                   ff.unit_name,
                   ff.street_address,
                   ff.province_abbr,
                   ff.country,
                   ff.postal_code,
                   aa.commencement_dat,
                   aa.document_chr,
                   aa.entered_dat,
                   aa.expiry_dat,
                   aa.location_dsc,
                   aa.offer_timber_deferment_yrn,
                   aa.received_dat,
                   aa.surface_or_under_cde,
                   aa.timber_value_dlr,
                   ss.ORGANIZATIONS_LEGAL_NAME,
                   ss.INDIVIDUALS_FIRST_NAME,
                   ss.INDIVIDUALS_LAST_NAME,
                   ss.CITY,
                   ss.REGION_CDE,
                   ss.COUNTRY_CDE,
                   zz.WORK_AREA_CODE,
                   zz.WORK_EXTENSION_NUMBER,
                   zz.WORK_PHONE_NUMBER
             from
                   whse_tantalis.ta_disposition_transactions aa,
                   whse_tantalis.ta_available_purposes bb,
                   whse_tantalis.ta_available_subpurposes cc,
                   whse_tantalis.ta_available_types dd,
                   whse_tantalis.ta_available_subtypes ee,
                   whse_tantalis.ta_organization_units ff,                             
                   WHSE_TANTALIS.TA_INTEREST_HOLDER_VW ss,
                   WHSE_TANTALIS.TA_INTERESTED_PARTIES zz
             where
                   ss.DISPOSITION_TRANSACTION_SID = aa.DISPOSITION_TRANSACTION_SID(+) and
                   zz.INTERESTED_PARTY_SID = ss.INTERESTED_PARTY_SID(+) and
                   aa.purpose_sid = bb.purpose_sid and
                   bb.purpose_sid = cc.purpose_sid and
                   aa.subpurpose_sid = cc.subpurpose_sid and
                   aa.type_sid = dd.type_sid and
                   dd.type_sid = ee.type_sid and
                   aa.subtype_sid = ee.subtype_sid and
                   aa.org_unit_sid = ff.org_unit_sid 
             order by
                   aa.disposition_transaction_sid) jj,
                   whse_tantalis.ta_dispositions kk,
                 (select gg.disposition_transaction_sid,
                 gg.effective_dat,
                 gg.code_chr_status,
                 hh.status_nme,
                 hh.activation_cde as dts_activation_cde,
                 hh.expiry_dat as status_expiry_dat,
                 gg.code_chr_stage,
                 ii.stage_nme,
                 gg.expiry_dat as stage_expiry_dat
            from
                 whse_tantalis.ta_disp_trans_statuses gg,
                 whse_tantalis.ta_status hh,
                 whse_tantalis.ta_stages ii
            where
                 gg.code_chr_status = hh.code_chr and
                 gg.code_chr_stage = ii.code_chr and
                 gg.expiry_dat is null
            order by
                 gg.disposition_transaction_sid) ll
              where
                   jj.disposition_sid = kk.disposition_sid(+) and
                   jj.disposition_transaction_sid = ll.disposition_transaction_sid(+)
              order by
                   jj.disposition_transaction_sid) nn,
   
                whse_tantalis.ta_interest_parcels mm,
                WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES xx
            where
                mm.disposition_transaction_sid(+) = nn.disposition_transaction_sid and
                xx.INTRID_SID = mm.INTRID_SID  and
                
                nn.dts_activation_cde = 'INACT' and  
                nn.stage_nme != 'APPLICATION' and
              
                SDO_RELATE (xx.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid_aoi),'mask=ANYINTERACT') = 'TRUE' and             
     
                nn.file_chr NOT IN (select ten.CROWN_LANDS_FILE
                                     from WHSE_TANTALIS.TA_CROWN_TENURES_VW ten
                                     where ten.TENURE_STATUS IN ('DISPOSITION IN GOOD STANDING', 'OFFERED', 'OFFER ACCEPTED'))
 
                          """    
    
    return sql