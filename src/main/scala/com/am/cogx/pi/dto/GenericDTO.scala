/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */

package com.am.cogx.pi.dto

/**
 * @author Yasin S
 * @version 1.0
 *
 * Generic DAO for persisting simple 1-1 entities into HBase
 *
 */


  @SerialVersionUID(2017L)
  case class CogxUMRecord(
  rfrnc_nbr: String,
  srvc_line_nbr: String,
  clncl_sor_cd: String,
  mbrshp_sor_cd: String,
  um_srvc_stts_cd: String,
  src_um_srvc_stts_cd: String,
  src_sbscrbr_id: String,
  src_mbr_cd: String,
  prmry_diag_cd: String,
  rqstd_place_of_srvc_cd: String,
  src_rqstd_place_of_srvc_cd: String,
  authrzd_place_of_srvc_cd: String,
  src_authrzd_place_of_srvc_cd: String,
  rqstd_srvc_from_dt: String,
  authrzd_srvc_from_dt: String,
  rqstd_srvc_to_dt: String,
  authrzd_srvc_to_dt: String,
  rqstd_proc_srvc_cd: String,
  authrzd_proc_srvc_cd: String,
  rqstd_qty: String,
  authrzd_qty: String,
  src_um_prov_id: String,
  prov_id: String,
  src_um_prov_id_rp: String,
  prov_id_rp: String,
  src_prov_frst_nm_rp: String,
  src_prov_last_nm_rp: String,
  src_prov_frst_nm: String,
  src_prov_last_nm: String)

case class cogxUmInfo(cogxUMdata: CogxUMRecord)

case class cogxUmHistory(cogxUM: Array[CogxUMRecord])




