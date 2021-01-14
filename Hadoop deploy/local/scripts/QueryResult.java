// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Fri Jun 14 14:22:26 EDT 2019
// For connector: org.apache.sqoop.manager.GenericJdbcManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("ADJDCTN_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ADJDCTN_DT = (java.sql.Date)value;
      }
    });
    setters.put("ADJDCTN_STTS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ADJDCTN_STTS = (String)value;
      }
    });
    setters.put("ADMSN_TYPE_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ADMSN_TYPE_CD = (String)value;
      }
    });
    setters.put("ADMT_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ADMT_DT = (java.sql.Date)value;
      }
    });
    setters.put("ALWD_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ALWD_AMT = (Double)value;
      }
    });
    setters.put("APC_MEDCR_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        APC_MEDCR_ID = (String)value;
      }
    });
    setters.put("APC_VRSN_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        APC_VRSN_NBR = (String)value;
      }
    });
    setters.put("BED_TYPE_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BED_TYPE_CD = (String)value;
      }
    });
    setters.put("BILLD_CHRG_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLD_CHRG_AMT = (Double)value;
      }
    });
    setters.put("BILLD_SRVC_UNIT_CNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLD_SRVC_UNIT_CNT = (Double)value;
      }
    });
    setters.put("BNFT_PAYMNT_STTS_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BNFT_PAYMNT_STTS_CD = (String)value;
      }
    });
    setters.put("CASES", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CASES = (java.math.BigDecimal)value;
      }
    });
    setters.put("CLM_ADJSTMNT_KEY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_ADJSTMNT_KEY = (String)value;
      }
    });
    setters.put("CLM_ADJSTMNT_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_ADJSTMNT_NBR = (java.math.BigDecimal)value;
      }
    });
    setters.put("CLM_DISP_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_DISP_CD = (String)value;
      }
    });
    setters.put("CLM_ITS_HOST_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_ITS_HOST_CD = (String)value;
      }
    });
    setters.put("CLM_LINE_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_LINE_NBR = (String)value;
      }
    });
    setters.put("CLM_LINE_REIMB_MTHD_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_LINE_REIMB_MTHD_CD = (String)value;
      }
    });
    setters.put("CLM_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_NBR = (String)value;
      }
    });
    setters.put("CLM_REIMBMNT_SOR_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_REIMBMNT_SOR_CD = (String)value;
      }
    });
    setters.put("CLM_REIMBMNT_TYPE_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_REIMBMNT_TYPE_CD = (String)value;
      }
    });
    setters.put("CLM_SOR_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_SOR_CD = (String)value;
      }
    });
    setters.put("CLM_STMNT_FROM_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_STMNT_FROM_DT = (java.sql.Date)value;
      }
    });
    setters.put("CLM_STMNT_TO_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_STMNT_TO_DT = (java.sql.Date)value;
      }
    });
    setters.put("CMAD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CMAD = (Double)value;
      }
    });
    setters.put("CMAD1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CMAD1 = (Double)value;
      }
    });
    setters.put("CMAD_ALLOWED", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CMAD_ALLOWED = (Double)value;
      }
    });
    setters.put("CMAD_BILLED", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CMAD_BILLED = (Double)value;
      }
    });
    setters.put("CMAD_CASES", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CMAD_CASES = (java.math.BigDecimal)value;
      }
    });
    setters.put("CMPNY_CF_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CMPNY_CF_CD = (String)value;
      }
    });
    setters.put("CVRD_EXPNS_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CVRD_EXPNS_AMT = (Double)value;
      }
    });
    setters.put("DERIVD_IND_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DERIVD_IND_CD = (String)value;
      }
    });
    setters.put("DSCHRG_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DSCHRG_DT = (java.sql.Date)value;
      }
    });
    setters.put("DSCHRG_STTS_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DSCHRG_STTS_CD = (String)value;
      }
    });
    setters.put("ER_Flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ER_Flag = (String)value;
      }
    });
    setters.put("PediatricFlag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PediatricFlag = (String)value;
      }
    });
    setters.put("ENC_Flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ENC_Flag = (String)value;
      }
    });
    setters.put("EXCHNG_CERTFN_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EXCHNG_CERTFN_CD = (String)value;
      }
    });
    setters.put("EXCHNG_IND_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EXCHNG_IND_CD = (String)value;
      }
    });
    setters.put("EXCHNG_METAL_TYPE_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EXCHNG_METAL_TYPE_CD = (String)value;
      }
    });
    setters.put("FUNDG_CF_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FUNDG_CF_CD = (String)value;
      }
    });
    setters.put("HLTH_SRVC_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        HLTH_SRVC_CD = (String)value;
      }
    });
    setters.put("PROC_MDFR_1_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROC_MDFR_1_CD = (String)value;
      }
    });
    setters.put("PROC_MDFR_2_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROC_MDFR_2_CD = (String)value;
      }
    });
    setters.put("HMO_CLS_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        HMO_CLS_CD = (String)value;
      }
    });
    setters.put("ICL_ALWD_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ICL_ALWD_AMT = (Double)value;
      }
    });
    setters.put("ICL_PAID_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ICL_PAID_AMT = (Double)value;
      }
    });
    setters.put("INN_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        INN_CD = (String)value;
      }
    });
    setters.put("LEGACY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        LEGACY = (String)value;
      }
    });
    setters.put("LINE_ITEM_APC_WT_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        LINE_ITEM_APC_WT_AMT = (Double)value;
      }
    });
    setters.put("LINE_ITEM_PAYMNT_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        LINE_ITEM_PAYMNT_AMT = (Double)value;
      }
    });
    setters.put("MBR_County", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBR_County = (String)value;
      }
    });
    setters.put("MBR_KEY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBR_KEY = (String)value;
      }
    });
    setters.put("MBR_State", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBR_State = (String)value;
      }
    });
    setters.put("MBR_ZIP3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBR_ZIP3 = (String)value;
      }
    });
    setters.put("MBR_ZIP_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBR_ZIP_CD = (String)value;
      }
    });
    setters.put("MBU_CF_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBU_CF_CD = (String)value;
      }
    });
    setters.put("MBUlvl1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBUlvl1 = (String)value;
      }
    });
    setters.put("MBUlvl2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBUlvl2 = (String)value;
      }
    });
    setters.put("MBUlvl3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBUlvl3 = (String)value;
      }
    });
    setters.put("MBUlvl4", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBUlvl4 = (String)value;
      }
    });
    setters.put("MCS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MCS = (String)value;
      }
    });
    setters.put("MEDCR_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MEDCR_ID = (String)value;
      }
    });
    setters.put("NRMLZD_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NRMLZD_AMT = (Double)value;
      }
    });
    setters.put("NRMLZD_CASE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NRMLZD_CASE = (java.math.BigDecimal)value;
      }
    });
    setters.put("NTWK_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NTWK_ID = (String)value;
      }
    });
    setters.put("CLM_NTWK_KEY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_NTWK_KEY = (String)value;
      }
    });
    setters.put("OBS_Flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OBS_Flag = (String)value;
      }
    });
    setters.put("OUT_ITEM_STTS_IND_TXT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OUT_ITEM_STTS_IND_TXT = (String)value;
      }
    });
    setters.put("PAID_AMT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PAID_AMT = (Double)value;
      }
    });
    setters.put("PAID_SRVC_UNIT_CNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PAID_SRVC_UNIT_CNT = (java.math.BigDecimal)value;
      }
    });
    setters.put("PARG_STTS_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PARG_STTS_CD = (String)value;
      }
    });
    setters.put("PAYMNT_APC_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PAYMNT_APC_NBR = (String)value;
      }
    });
    setters.put("PN_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PN_ID = (String)value;
      }
    });
    setters.put("PROD_CF_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_CF_CD = (String)value;
      }
    });
    setters.put("PROD_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROD_ID = (String)value;
      }
    });
    setters.put("PROV_CITY_NM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROV_CITY_NM = (String)value;
      }
    });
    setters.put("PROV_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROV_ID = (String)value;
      }
    });
    setters.put("PROV_NM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROV_NM = (String)value;
      }
    });
    setters.put("PROV_ST_NM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROV_ST_NM = (String)value;
      }
    });
    setters.put("PROV_ZIP_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROV_ZIP_CD = (String)value;
      }
    });
    setters.put("RDCTN_CTGRY_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RDCTN_CTGRY_CD = (String)value;
      }
    });
    setters.put("RELATIVE_WEIGHT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RELATIVE_WEIGHT = (Double)value;
      }
    });
    setters.put("RELATIVE_WEIGHT1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RELATIVE_WEIGHT1 = (Double)value;
      }
    });
    setters.put("RNDRG_PROV_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RNDRG_PROV_ID = (String)value;
      }
    });
    setters.put("RNDRG_PROV_ID_TYPE_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RNDRG_PROV_ID_TYPE_CD = (String)value;
      }
    });
    setters.put("RVNU_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RVNU_CD = (String)value;
      }
    });
    setters.put("RVU_Flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RVU_Flag = (String)value;
      }
    });
    setters.put("SRC_APRVL_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_APRVL_CD = (String)value;
      }
    });
    setters.put("SRC_BILLG_TAX_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_BILLG_TAX_ID = (String)value;
      }
    });
    setters.put("SRC_CLM_DISP_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_CLM_DISP_CD = (String)value;
      }
    });
    setters.put("SRC_FINDER_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_FINDER_NBR = (java.math.BigDecimal)value;
      }
    });
    setters.put("SRC_GRP_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_GRP_NBR = (String)value;
      }
    });
    setters.put("SRC_HMO_CLS_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_HMO_CLS_CD = (String)value;
      }
    });
    setters.put("SRC_INSTNL_NGTTD_SRVC_TERM_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_INSTNL_NGTTD_SRVC_TERM_ID = (String)value;
      }
    });
    setters.put("SRC_INSTNL_REIMBMNT_TERM_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_INSTNL_REIMBMNT_TERM_ID = (String)value;
      }
    });
    setters.put("SRC_MEDCR_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_MEDCR_ID = (String)value;
      }
    });
    setters.put("SRC_NST_SRVC_CTGRY_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_NST_SRVC_CTGRY_CD = (String)value;
      }
    });
    setters.put("SRC_PAY_ACTN_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_PAY_ACTN_CD = (String)value;
      }
    });
    setters.put("SRC_PRCG_CRTRIA_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_PRCG_CRTRIA_CD = (String)value;
      }
    });
    setters.put("SRC_PRCG_RSN_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_PRCG_RSN_CD = (String)value;
      }
    });
    setters.put("SRC_PROV_NATL_PROV_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_PROV_NATL_PROV_ID = (String)value;
      }
    });
    setters.put("SRC_PRTY_SCOR_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_PRTY_SCOR_NBR = (java.math.BigDecimal)value;
      }
    });
    setters.put("SRC_SRVC_CLSFCTN_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_SRVC_CLSFCTN_CD = (String)value;
      }
    });
    setters.put("SRC_TERM_EFCTV_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_TERM_EFCTV_DT = (java.sql.Date)value;
      }
    });
    setters.put("SRC_TERM_END_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_TERM_END_DT = (java.sql.Date)value;
      }
    });
    setters.put("TOS_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TOS_CD = (String)value;
      }
    });
    setters.put("apcst", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        apcst = (String)value;
      }
    });
    setters.put("apcwt", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        apcwt = (Double)value;
      }
    });
    setters.put("betos", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        betos = (String)value;
      }
    });
    setters.put("brand", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        brand = (String)value;
      }
    });
    setters.put("cat1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        cat1 = (String)value;
      }
    });
    setters.put("cat2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        cat2 = (String)value;
      }
    });
    setters.put("clmwt", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        clmwt = (Double)value;
      }
    });
    setters.put("erwt", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        erwt = (Double)value;
      }
    });
    setters.put("fundlvl2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fundlvl2 = (String)value;
      }
    });
    setters.put("liccd", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        liccd = (String)value;
      }
    });
    setters.put("market", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        market = (String)value;
      }
    });
    setters.put("Inc_Month", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Inc_Month = (String)value;
      }
    });
    setters.put("network", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        network = (String)value;
      }
    });
    setters.put("normflag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        normflag = (String)value;
      }
    });
    setters.put("priority", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        priority = (String)value;
      }
    });
    setters.put("prodlvl3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        prodlvl3 = (String)value;
      }
    });
    setters.put("prov_county", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        prov_county = (String)value;
      }
    });
    setters.put("surgrp", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        surgrp = (String)value;
      }
    });
    setters.put("surgwt", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        surgwt = (Double)value;
      }
    });
    setters.put("system_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        system_id = (String)value;
      }
    });
    setters.put("GRP_NM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        GRP_NM = (String)value;
      }
    });
    setters.put("SRC_SUBGRP_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_SUBGRP_NBR = (String)value;
      }
    });
    setters.put("VISITS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        VISITS = (Double)value;
      }
    });
    setters.put("GRP_SIC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        GRP_SIC = (String)value;
      }
    });
    setters.put("MCID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MCID = (String)value;
      }
    });
    setters.put("CLM_ITS_SCCF_NBR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_ITS_SCCF_NBR = (String)value;
      }
    });
    setters.put("MBRSHP_SOR_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MBRSHP_SOR_CD = (String)value;
      }
    });
    setters.put("SRC_RT_CTGRY_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_RT_CTGRY_CD = (String)value;
      }
    });
    setters.put("SRC_RT_CTGRY_SUB_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SRC_RT_CTGRY_SUB_CD = (String)value;
      }
    });
    setters.put("run_date", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        run_date = (java.sql.Date)value;
      }
    });
    setters.put("CLM_LINE_SRVC_STRT_DT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLM_LINE_SRVC_STRT_DT = (java.sql.Date)value;
      }
    });
    setters.put("VNDR_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        VNDR_CD = (String)value;
      }
    });
    setters.put("REPRICE_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REPRICE_CD = (String)value;
      }
    });
    setters.put("VNDR_PROD_CD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        VNDR_PROD_CD = (String)value;
      }
    });
    setters.put("zip3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        zip3 = (String)value;
      }
    });
  }
  public QueryResult() {
    init0();
  }
  private java.sql.Date ADJDCTN_DT;
  public java.sql.Date get_ADJDCTN_DT() {
    return ADJDCTN_DT;
  }
  public void set_ADJDCTN_DT(java.sql.Date ADJDCTN_DT) {
    this.ADJDCTN_DT = ADJDCTN_DT;
  }
  public QueryResult with_ADJDCTN_DT(java.sql.Date ADJDCTN_DT) {
    this.ADJDCTN_DT = ADJDCTN_DT;
    return this;
  }
  private String ADJDCTN_STTS;
  public String get_ADJDCTN_STTS() {
    return ADJDCTN_STTS;
  }
  public void set_ADJDCTN_STTS(String ADJDCTN_STTS) {
    this.ADJDCTN_STTS = ADJDCTN_STTS;
  }
  public QueryResult with_ADJDCTN_STTS(String ADJDCTN_STTS) {
    this.ADJDCTN_STTS = ADJDCTN_STTS;
    return this;
  }
  private String ADMSN_TYPE_CD;
  public String get_ADMSN_TYPE_CD() {
    return ADMSN_TYPE_CD;
  }
  public void set_ADMSN_TYPE_CD(String ADMSN_TYPE_CD) {
    this.ADMSN_TYPE_CD = ADMSN_TYPE_CD;
  }
  public QueryResult with_ADMSN_TYPE_CD(String ADMSN_TYPE_CD) {
    this.ADMSN_TYPE_CD = ADMSN_TYPE_CD;
    return this;
  }
  private java.sql.Date ADMT_DT;
  public java.sql.Date get_ADMT_DT() {
    return ADMT_DT;
  }
  public void set_ADMT_DT(java.sql.Date ADMT_DT) {
    this.ADMT_DT = ADMT_DT;
  }
  public QueryResult with_ADMT_DT(java.sql.Date ADMT_DT) {
    this.ADMT_DT = ADMT_DT;
    return this;
  }
  private Double ALWD_AMT;
  public Double get_ALWD_AMT() {
    return ALWD_AMT;
  }
  public void set_ALWD_AMT(Double ALWD_AMT) {
    this.ALWD_AMT = ALWD_AMT;
  }
  public QueryResult with_ALWD_AMT(Double ALWD_AMT) {
    this.ALWD_AMT = ALWD_AMT;
    return this;
  }
  private String APC_MEDCR_ID;
  public String get_APC_MEDCR_ID() {
    return APC_MEDCR_ID;
  }
  public void set_APC_MEDCR_ID(String APC_MEDCR_ID) {
    this.APC_MEDCR_ID = APC_MEDCR_ID;
  }
  public QueryResult with_APC_MEDCR_ID(String APC_MEDCR_ID) {
    this.APC_MEDCR_ID = APC_MEDCR_ID;
    return this;
  }
  private String APC_VRSN_NBR;
  public String get_APC_VRSN_NBR() {
    return APC_VRSN_NBR;
  }
  public void set_APC_VRSN_NBR(String APC_VRSN_NBR) {
    this.APC_VRSN_NBR = APC_VRSN_NBR;
  }
  public QueryResult with_APC_VRSN_NBR(String APC_VRSN_NBR) {
    this.APC_VRSN_NBR = APC_VRSN_NBR;
    return this;
  }
  private String BED_TYPE_CD;
  public String get_BED_TYPE_CD() {
    return BED_TYPE_CD;
  }
  public void set_BED_TYPE_CD(String BED_TYPE_CD) {
    this.BED_TYPE_CD = BED_TYPE_CD;
  }
  public QueryResult with_BED_TYPE_CD(String BED_TYPE_CD) {
    this.BED_TYPE_CD = BED_TYPE_CD;
    return this;
  }
  private Double BILLD_CHRG_AMT;
  public Double get_BILLD_CHRG_AMT() {
    return BILLD_CHRG_AMT;
  }
  public void set_BILLD_CHRG_AMT(Double BILLD_CHRG_AMT) {
    this.BILLD_CHRG_AMT = BILLD_CHRG_AMT;
  }
  public QueryResult with_BILLD_CHRG_AMT(Double BILLD_CHRG_AMT) {
    this.BILLD_CHRG_AMT = BILLD_CHRG_AMT;
    return this;
  }
  private Double BILLD_SRVC_UNIT_CNT;
  public Double get_BILLD_SRVC_UNIT_CNT() {
    return BILLD_SRVC_UNIT_CNT;
  }
  public void set_BILLD_SRVC_UNIT_CNT(Double BILLD_SRVC_UNIT_CNT) {
    this.BILLD_SRVC_UNIT_CNT = BILLD_SRVC_UNIT_CNT;
  }
  public QueryResult with_BILLD_SRVC_UNIT_CNT(Double BILLD_SRVC_UNIT_CNT) {
    this.BILLD_SRVC_UNIT_CNT = BILLD_SRVC_UNIT_CNT;
    return this;
  }
  private String BNFT_PAYMNT_STTS_CD;
  public String get_BNFT_PAYMNT_STTS_CD() {
    return BNFT_PAYMNT_STTS_CD;
  }
  public void set_BNFT_PAYMNT_STTS_CD(String BNFT_PAYMNT_STTS_CD) {
    this.BNFT_PAYMNT_STTS_CD = BNFT_PAYMNT_STTS_CD;
  }
  public QueryResult with_BNFT_PAYMNT_STTS_CD(String BNFT_PAYMNT_STTS_CD) {
    this.BNFT_PAYMNT_STTS_CD = BNFT_PAYMNT_STTS_CD;
    return this;
  }
  private java.math.BigDecimal CASES;
  public java.math.BigDecimal get_CASES() {
    return CASES;
  }
  public void set_CASES(java.math.BigDecimal CASES) {
    this.CASES = CASES;
  }
  public QueryResult with_CASES(java.math.BigDecimal CASES) {
    this.CASES = CASES;
    return this;
  }
  private String CLM_ADJSTMNT_KEY;
  public String get_CLM_ADJSTMNT_KEY() {
    return CLM_ADJSTMNT_KEY;
  }
  public void set_CLM_ADJSTMNT_KEY(String CLM_ADJSTMNT_KEY) {
    this.CLM_ADJSTMNT_KEY = CLM_ADJSTMNT_KEY;
  }
  public QueryResult with_CLM_ADJSTMNT_KEY(String CLM_ADJSTMNT_KEY) {
    this.CLM_ADJSTMNT_KEY = CLM_ADJSTMNT_KEY;
    return this;
  }
  private java.math.BigDecimal CLM_ADJSTMNT_NBR;
  public java.math.BigDecimal get_CLM_ADJSTMNT_NBR() {
    return CLM_ADJSTMNT_NBR;
  }
  public void set_CLM_ADJSTMNT_NBR(java.math.BigDecimal CLM_ADJSTMNT_NBR) {
    this.CLM_ADJSTMNT_NBR = CLM_ADJSTMNT_NBR;
  }
  public QueryResult with_CLM_ADJSTMNT_NBR(java.math.BigDecimal CLM_ADJSTMNT_NBR) {
    this.CLM_ADJSTMNT_NBR = CLM_ADJSTMNT_NBR;
    return this;
  }
  private String CLM_DISP_CD;
  public String get_CLM_DISP_CD() {
    return CLM_DISP_CD;
  }
  public void set_CLM_DISP_CD(String CLM_DISP_CD) {
    this.CLM_DISP_CD = CLM_DISP_CD;
  }
  public QueryResult with_CLM_DISP_CD(String CLM_DISP_CD) {
    this.CLM_DISP_CD = CLM_DISP_CD;
    return this;
  }
  private String CLM_ITS_HOST_CD;
  public String get_CLM_ITS_HOST_CD() {
    return CLM_ITS_HOST_CD;
  }
  public void set_CLM_ITS_HOST_CD(String CLM_ITS_HOST_CD) {
    this.CLM_ITS_HOST_CD = CLM_ITS_HOST_CD;
  }
  public QueryResult with_CLM_ITS_HOST_CD(String CLM_ITS_HOST_CD) {
    this.CLM_ITS_HOST_CD = CLM_ITS_HOST_CD;
    return this;
  }
  private String CLM_LINE_NBR;
  public String get_CLM_LINE_NBR() {
    return CLM_LINE_NBR;
  }
  public void set_CLM_LINE_NBR(String CLM_LINE_NBR) {
    this.CLM_LINE_NBR = CLM_LINE_NBR;
  }
  public QueryResult with_CLM_LINE_NBR(String CLM_LINE_NBR) {
    this.CLM_LINE_NBR = CLM_LINE_NBR;
    return this;
  }
  private String CLM_LINE_REIMB_MTHD_CD;
  public String get_CLM_LINE_REIMB_MTHD_CD() {
    return CLM_LINE_REIMB_MTHD_CD;
  }
  public void set_CLM_LINE_REIMB_MTHD_CD(String CLM_LINE_REIMB_MTHD_CD) {
    this.CLM_LINE_REIMB_MTHD_CD = CLM_LINE_REIMB_MTHD_CD;
  }
  public QueryResult with_CLM_LINE_REIMB_MTHD_CD(String CLM_LINE_REIMB_MTHD_CD) {
    this.CLM_LINE_REIMB_MTHD_CD = CLM_LINE_REIMB_MTHD_CD;
    return this;
  }
  private String CLM_NBR;
  public String get_CLM_NBR() {
    return CLM_NBR;
  }
  public void set_CLM_NBR(String CLM_NBR) {
    this.CLM_NBR = CLM_NBR;
  }
  public QueryResult with_CLM_NBR(String CLM_NBR) {
    this.CLM_NBR = CLM_NBR;
    return this;
  }
  private String CLM_REIMBMNT_SOR_CD;
  public String get_CLM_REIMBMNT_SOR_CD() {
    return CLM_REIMBMNT_SOR_CD;
  }
  public void set_CLM_REIMBMNT_SOR_CD(String CLM_REIMBMNT_SOR_CD) {
    this.CLM_REIMBMNT_SOR_CD = CLM_REIMBMNT_SOR_CD;
  }
  public QueryResult with_CLM_REIMBMNT_SOR_CD(String CLM_REIMBMNT_SOR_CD) {
    this.CLM_REIMBMNT_SOR_CD = CLM_REIMBMNT_SOR_CD;
    return this;
  }
  private String CLM_REIMBMNT_TYPE_CD;
  public String get_CLM_REIMBMNT_TYPE_CD() {
    return CLM_REIMBMNT_TYPE_CD;
  }
  public void set_CLM_REIMBMNT_TYPE_CD(String CLM_REIMBMNT_TYPE_CD) {
    this.CLM_REIMBMNT_TYPE_CD = CLM_REIMBMNT_TYPE_CD;
  }
  public QueryResult with_CLM_REIMBMNT_TYPE_CD(String CLM_REIMBMNT_TYPE_CD) {
    this.CLM_REIMBMNT_TYPE_CD = CLM_REIMBMNT_TYPE_CD;
    return this;
  }
  private String CLM_SOR_CD;
  public String get_CLM_SOR_CD() {
    return CLM_SOR_CD;
  }
  public void set_CLM_SOR_CD(String CLM_SOR_CD) {
    this.CLM_SOR_CD = CLM_SOR_CD;
  }
  public QueryResult with_CLM_SOR_CD(String CLM_SOR_CD) {
    this.CLM_SOR_CD = CLM_SOR_CD;
    return this;
  }
  private java.sql.Date CLM_STMNT_FROM_DT;
  public java.sql.Date get_CLM_STMNT_FROM_DT() {
    return CLM_STMNT_FROM_DT;
  }
  public void set_CLM_STMNT_FROM_DT(java.sql.Date CLM_STMNT_FROM_DT) {
    this.CLM_STMNT_FROM_DT = CLM_STMNT_FROM_DT;
  }
  public QueryResult with_CLM_STMNT_FROM_DT(java.sql.Date CLM_STMNT_FROM_DT) {
    this.CLM_STMNT_FROM_DT = CLM_STMNT_FROM_DT;
    return this;
  }
  private java.sql.Date CLM_STMNT_TO_DT;
  public java.sql.Date get_CLM_STMNT_TO_DT() {
    return CLM_STMNT_TO_DT;
  }
  public void set_CLM_STMNT_TO_DT(java.sql.Date CLM_STMNT_TO_DT) {
    this.CLM_STMNT_TO_DT = CLM_STMNT_TO_DT;
  }
  public QueryResult with_CLM_STMNT_TO_DT(java.sql.Date CLM_STMNT_TO_DT) {
    this.CLM_STMNT_TO_DT = CLM_STMNT_TO_DT;
    return this;
  }
  private Double CMAD;
  public Double get_CMAD() {
    return CMAD;
  }
  public void set_CMAD(Double CMAD) {
    this.CMAD = CMAD;
  }
  public QueryResult with_CMAD(Double CMAD) {
    this.CMAD = CMAD;
    return this;
  }
  private Double CMAD1;
  public Double get_CMAD1() {
    return CMAD1;
  }
  public void set_CMAD1(Double CMAD1) {
    this.CMAD1 = CMAD1;
  }
  public QueryResult with_CMAD1(Double CMAD1) {
    this.CMAD1 = CMAD1;
    return this;
  }
  private Double CMAD_ALLOWED;
  public Double get_CMAD_ALLOWED() {
    return CMAD_ALLOWED;
  }
  public void set_CMAD_ALLOWED(Double CMAD_ALLOWED) {
    this.CMAD_ALLOWED = CMAD_ALLOWED;
  }
  public QueryResult with_CMAD_ALLOWED(Double CMAD_ALLOWED) {
    this.CMAD_ALLOWED = CMAD_ALLOWED;
    return this;
  }
  private Double CMAD_BILLED;
  public Double get_CMAD_BILLED() {
    return CMAD_BILLED;
  }
  public void set_CMAD_BILLED(Double CMAD_BILLED) {
    this.CMAD_BILLED = CMAD_BILLED;
  }
  public QueryResult with_CMAD_BILLED(Double CMAD_BILLED) {
    this.CMAD_BILLED = CMAD_BILLED;
    return this;
  }
  private java.math.BigDecimal CMAD_CASES;
  public java.math.BigDecimal get_CMAD_CASES() {
    return CMAD_CASES;
  }
  public void set_CMAD_CASES(java.math.BigDecimal CMAD_CASES) {
    this.CMAD_CASES = CMAD_CASES;
  }
  public QueryResult with_CMAD_CASES(java.math.BigDecimal CMAD_CASES) {
    this.CMAD_CASES = CMAD_CASES;
    return this;
  }
  private String CMPNY_CF_CD;
  public String get_CMPNY_CF_CD() {
    return CMPNY_CF_CD;
  }
  public void set_CMPNY_CF_CD(String CMPNY_CF_CD) {
    this.CMPNY_CF_CD = CMPNY_CF_CD;
  }
  public QueryResult with_CMPNY_CF_CD(String CMPNY_CF_CD) {
    this.CMPNY_CF_CD = CMPNY_CF_CD;
    return this;
  }
  private Double CVRD_EXPNS_AMT;
  public Double get_CVRD_EXPNS_AMT() {
    return CVRD_EXPNS_AMT;
  }
  public void set_CVRD_EXPNS_AMT(Double CVRD_EXPNS_AMT) {
    this.CVRD_EXPNS_AMT = CVRD_EXPNS_AMT;
  }
  public QueryResult with_CVRD_EXPNS_AMT(Double CVRD_EXPNS_AMT) {
    this.CVRD_EXPNS_AMT = CVRD_EXPNS_AMT;
    return this;
  }
  private String DERIVD_IND_CD;
  public String get_DERIVD_IND_CD() {
    return DERIVD_IND_CD;
  }
  public void set_DERIVD_IND_CD(String DERIVD_IND_CD) {
    this.DERIVD_IND_CD = DERIVD_IND_CD;
  }
  public QueryResult with_DERIVD_IND_CD(String DERIVD_IND_CD) {
    this.DERIVD_IND_CD = DERIVD_IND_CD;
    return this;
  }
  private java.sql.Date DSCHRG_DT;
  public java.sql.Date get_DSCHRG_DT() {
    return DSCHRG_DT;
  }
  public void set_DSCHRG_DT(java.sql.Date DSCHRG_DT) {
    this.DSCHRG_DT = DSCHRG_DT;
  }
  public QueryResult with_DSCHRG_DT(java.sql.Date DSCHRG_DT) {
    this.DSCHRG_DT = DSCHRG_DT;
    return this;
  }
  private String DSCHRG_STTS_CD;
  public String get_DSCHRG_STTS_CD() {
    return DSCHRG_STTS_CD;
  }
  public void set_DSCHRG_STTS_CD(String DSCHRG_STTS_CD) {
    this.DSCHRG_STTS_CD = DSCHRG_STTS_CD;
  }
  public QueryResult with_DSCHRG_STTS_CD(String DSCHRG_STTS_CD) {
    this.DSCHRG_STTS_CD = DSCHRG_STTS_CD;
    return this;
  }
  private String ER_Flag;
  public String get_ER_Flag() {
    return ER_Flag;
  }
  public void set_ER_Flag(String ER_Flag) {
    this.ER_Flag = ER_Flag;
  }
  public QueryResult with_ER_Flag(String ER_Flag) {
    this.ER_Flag = ER_Flag;
    return this;
  }
  private String PediatricFlag;
  public String get_PediatricFlag() {
    return PediatricFlag;
  }
  public void set_PediatricFlag(String PediatricFlag) {
    this.PediatricFlag = PediatricFlag;
  }
  public QueryResult with_PediatricFlag(String PediatricFlag) {
    this.PediatricFlag = PediatricFlag;
    return this;
  }
  private String ENC_Flag;
  public String get_ENC_Flag() {
    return ENC_Flag;
  }
  public void set_ENC_Flag(String ENC_Flag) {
    this.ENC_Flag = ENC_Flag;
  }
  public QueryResult with_ENC_Flag(String ENC_Flag) {
    this.ENC_Flag = ENC_Flag;
    return this;
  }
  private String EXCHNG_CERTFN_CD;
  public String get_EXCHNG_CERTFN_CD() {
    return EXCHNG_CERTFN_CD;
  }
  public void set_EXCHNG_CERTFN_CD(String EXCHNG_CERTFN_CD) {
    this.EXCHNG_CERTFN_CD = EXCHNG_CERTFN_CD;
  }
  public QueryResult with_EXCHNG_CERTFN_CD(String EXCHNG_CERTFN_CD) {
    this.EXCHNG_CERTFN_CD = EXCHNG_CERTFN_CD;
    return this;
  }
  private String EXCHNG_IND_CD;
  public String get_EXCHNG_IND_CD() {
    return EXCHNG_IND_CD;
  }
  public void set_EXCHNG_IND_CD(String EXCHNG_IND_CD) {
    this.EXCHNG_IND_CD = EXCHNG_IND_CD;
  }
  public QueryResult with_EXCHNG_IND_CD(String EXCHNG_IND_CD) {
    this.EXCHNG_IND_CD = EXCHNG_IND_CD;
    return this;
  }
  private String EXCHNG_METAL_TYPE_CD;
  public String get_EXCHNG_METAL_TYPE_CD() {
    return EXCHNG_METAL_TYPE_CD;
  }
  public void set_EXCHNG_METAL_TYPE_CD(String EXCHNG_METAL_TYPE_CD) {
    this.EXCHNG_METAL_TYPE_CD = EXCHNG_METAL_TYPE_CD;
  }
  public QueryResult with_EXCHNG_METAL_TYPE_CD(String EXCHNG_METAL_TYPE_CD) {
    this.EXCHNG_METAL_TYPE_CD = EXCHNG_METAL_TYPE_CD;
    return this;
  }
  private String FUNDG_CF_CD;
  public String get_FUNDG_CF_CD() {
    return FUNDG_CF_CD;
  }
  public void set_FUNDG_CF_CD(String FUNDG_CF_CD) {
    this.FUNDG_CF_CD = FUNDG_CF_CD;
  }
  public QueryResult with_FUNDG_CF_CD(String FUNDG_CF_CD) {
    this.FUNDG_CF_CD = FUNDG_CF_CD;
    return this;
  }
  private String HLTH_SRVC_CD;
  public String get_HLTH_SRVC_CD() {
    return HLTH_SRVC_CD;
  }
  public void set_HLTH_SRVC_CD(String HLTH_SRVC_CD) {
    this.HLTH_SRVC_CD = HLTH_SRVC_CD;
  }
  public QueryResult with_HLTH_SRVC_CD(String HLTH_SRVC_CD) {
    this.HLTH_SRVC_CD = HLTH_SRVC_CD;
    return this;
  }
  private String PROC_MDFR_1_CD;
  public String get_PROC_MDFR_1_CD() {
    return PROC_MDFR_1_CD;
  }
  public void set_PROC_MDFR_1_CD(String PROC_MDFR_1_CD) {
    this.PROC_MDFR_1_CD = PROC_MDFR_1_CD;
  }
  public QueryResult with_PROC_MDFR_1_CD(String PROC_MDFR_1_CD) {
    this.PROC_MDFR_1_CD = PROC_MDFR_1_CD;
    return this;
  }
  private String PROC_MDFR_2_CD;
  public String get_PROC_MDFR_2_CD() {
    return PROC_MDFR_2_CD;
  }
  public void set_PROC_MDFR_2_CD(String PROC_MDFR_2_CD) {
    this.PROC_MDFR_2_CD = PROC_MDFR_2_CD;
  }
  public QueryResult with_PROC_MDFR_2_CD(String PROC_MDFR_2_CD) {
    this.PROC_MDFR_2_CD = PROC_MDFR_2_CD;
    return this;
  }
  private String HMO_CLS_CD;
  public String get_HMO_CLS_CD() {
    return HMO_CLS_CD;
  }
  public void set_HMO_CLS_CD(String HMO_CLS_CD) {
    this.HMO_CLS_CD = HMO_CLS_CD;
  }
  public QueryResult with_HMO_CLS_CD(String HMO_CLS_CD) {
    this.HMO_CLS_CD = HMO_CLS_CD;
    return this;
  }
  private Double ICL_ALWD_AMT;
  public Double get_ICL_ALWD_AMT() {
    return ICL_ALWD_AMT;
  }
  public void set_ICL_ALWD_AMT(Double ICL_ALWD_AMT) {
    this.ICL_ALWD_AMT = ICL_ALWD_AMT;
  }
  public QueryResult with_ICL_ALWD_AMT(Double ICL_ALWD_AMT) {
    this.ICL_ALWD_AMT = ICL_ALWD_AMT;
    return this;
  }
  private Double ICL_PAID_AMT;
  public Double get_ICL_PAID_AMT() {
    return ICL_PAID_AMT;
  }
  public void set_ICL_PAID_AMT(Double ICL_PAID_AMT) {
    this.ICL_PAID_AMT = ICL_PAID_AMT;
  }
  public QueryResult with_ICL_PAID_AMT(Double ICL_PAID_AMT) {
    this.ICL_PAID_AMT = ICL_PAID_AMT;
    return this;
  }
  private String INN_CD;
  public String get_INN_CD() {
    return INN_CD;
  }
  public void set_INN_CD(String INN_CD) {
    this.INN_CD = INN_CD;
  }
  public QueryResult with_INN_CD(String INN_CD) {
    this.INN_CD = INN_CD;
    return this;
  }
  private String LEGACY;
  public String get_LEGACY() {
    return LEGACY;
  }
  public void set_LEGACY(String LEGACY) {
    this.LEGACY = LEGACY;
  }
  public QueryResult with_LEGACY(String LEGACY) {
    this.LEGACY = LEGACY;
    return this;
  }
  private Double LINE_ITEM_APC_WT_AMT;
  public Double get_LINE_ITEM_APC_WT_AMT() {
    return LINE_ITEM_APC_WT_AMT;
  }
  public void set_LINE_ITEM_APC_WT_AMT(Double LINE_ITEM_APC_WT_AMT) {
    this.LINE_ITEM_APC_WT_AMT = LINE_ITEM_APC_WT_AMT;
  }
  public QueryResult with_LINE_ITEM_APC_WT_AMT(Double LINE_ITEM_APC_WT_AMT) {
    this.LINE_ITEM_APC_WT_AMT = LINE_ITEM_APC_WT_AMT;
    return this;
  }
  private Double LINE_ITEM_PAYMNT_AMT;
  public Double get_LINE_ITEM_PAYMNT_AMT() {
    return LINE_ITEM_PAYMNT_AMT;
  }
  public void set_LINE_ITEM_PAYMNT_AMT(Double LINE_ITEM_PAYMNT_AMT) {
    this.LINE_ITEM_PAYMNT_AMT = LINE_ITEM_PAYMNT_AMT;
  }
  public QueryResult with_LINE_ITEM_PAYMNT_AMT(Double LINE_ITEM_PAYMNT_AMT) {
    this.LINE_ITEM_PAYMNT_AMT = LINE_ITEM_PAYMNT_AMT;
    return this;
  }
  private String MBR_County;
  public String get_MBR_County() {
    return MBR_County;
  }
  public void set_MBR_County(String MBR_County) {
    this.MBR_County = MBR_County;
  }
  public QueryResult with_MBR_County(String MBR_County) {
    this.MBR_County = MBR_County;
    return this;
  }
  private String MBR_KEY;
  public String get_MBR_KEY() {
    return MBR_KEY;
  }
  public void set_MBR_KEY(String MBR_KEY) {
    this.MBR_KEY = MBR_KEY;
  }
  public QueryResult with_MBR_KEY(String MBR_KEY) {
    this.MBR_KEY = MBR_KEY;
    return this;
  }
  private String MBR_State;
  public String get_MBR_State() {
    return MBR_State;
  }
  public void set_MBR_State(String MBR_State) {
    this.MBR_State = MBR_State;
  }
  public QueryResult with_MBR_State(String MBR_State) {
    this.MBR_State = MBR_State;
    return this;
  }
  private String MBR_ZIP3;
  public String get_MBR_ZIP3() {
    return MBR_ZIP3;
  }
  public void set_MBR_ZIP3(String MBR_ZIP3) {
    this.MBR_ZIP3 = MBR_ZIP3;
  }
  public QueryResult with_MBR_ZIP3(String MBR_ZIP3) {
    this.MBR_ZIP3 = MBR_ZIP3;
    return this;
  }
  private String MBR_ZIP_CD;
  public String get_MBR_ZIP_CD() {
    return MBR_ZIP_CD;
  }
  public void set_MBR_ZIP_CD(String MBR_ZIP_CD) {
    this.MBR_ZIP_CD = MBR_ZIP_CD;
  }
  public QueryResult with_MBR_ZIP_CD(String MBR_ZIP_CD) {
    this.MBR_ZIP_CD = MBR_ZIP_CD;
    return this;
  }
  private String MBU_CF_CD;
  public String get_MBU_CF_CD() {
    return MBU_CF_CD;
  }
  public void set_MBU_CF_CD(String MBU_CF_CD) {
    this.MBU_CF_CD = MBU_CF_CD;
  }
  public QueryResult with_MBU_CF_CD(String MBU_CF_CD) {
    this.MBU_CF_CD = MBU_CF_CD;
    return this;
  }
  private String MBUlvl1;
  public String get_MBUlvl1() {
    return MBUlvl1;
  }
  public void set_MBUlvl1(String MBUlvl1) {
    this.MBUlvl1 = MBUlvl1;
  }
  public QueryResult with_MBUlvl1(String MBUlvl1) {
    this.MBUlvl1 = MBUlvl1;
    return this;
  }
  private String MBUlvl2;
  public String get_MBUlvl2() {
    return MBUlvl2;
  }
  public void set_MBUlvl2(String MBUlvl2) {
    this.MBUlvl2 = MBUlvl2;
  }
  public QueryResult with_MBUlvl2(String MBUlvl2) {
    this.MBUlvl2 = MBUlvl2;
    return this;
  }
  private String MBUlvl3;
  public String get_MBUlvl3() {
    return MBUlvl3;
  }
  public void set_MBUlvl3(String MBUlvl3) {
    this.MBUlvl3 = MBUlvl3;
  }
  public QueryResult with_MBUlvl3(String MBUlvl3) {
    this.MBUlvl3 = MBUlvl3;
    return this;
  }
  private String MBUlvl4;
  public String get_MBUlvl4() {
    return MBUlvl4;
  }
  public void set_MBUlvl4(String MBUlvl4) {
    this.MBUlvl4 = MBUlvl4;
  }
  public QueryResult with_MBUlvl4(String MBUlvl4) {
    this.MBUlvl4 = MBUlvl4;
    return this;
  }
  private String MCS;
  public String get_MCS() {
    return MCS;
  }
  public void set_MCS(String MCS) {
    this.MCS = MCS;
  }
  public QueryResult with_MCS(String MCS) {
    this.MCS = MCS;
    return this;
  }
  private String MEDCR_ID;
  public String get_MEDCR_ID() {
    return MEDCR_ID;
  }
  public void set_MEDCR_ID(String MEDCR_ID) {
    this.MEDCR_ID = MEDCR_ID;
  }
  public QueryResult with_MEDCR_ID(String MEDCR_ID) {
    this.MEDCR_ID = MEDCR_ID;
    return this;
  }
  private Double NRMLZD_AMT;
  public Double get_NRMLZD_AMT() {
    return NRMLZD_AMT;
  }
  public void set_NRMLZD_AMT(Double NRMLZD_AMT) {
    this.NRMLZD_AMT = NRMLZD_AMT;
  }
  public QueryResult with_NRMLZD_AMT(Double NRMLZD_AMT) {
    this.NRMLZD_AMT = NRMLZD_AMT;
    return this;
  }
  private java.math.BigDecimal NRMLZD_CASE;
  public java.math.BigDecimal get_NRMLZD_CASE() {
    return NRMLZD_CASE;
  }
  public void set_NRMLZD_CASE(java.math.BigDecimal NRMLZD_CASE) {
    this.NRMLZD_CASE = NRMLZD_CASE;
  }
  public QueryResult with_NRMLZD_CASE(java.math.BigDecimal NRMLZD_CASE) {
    this.NRMLZD_CASE = NRMLZD_CASE;
    return this;
  }
  private String NTWK_ID;
  public String get_NTWK_ID() {
    return NTWK_ID;
  }
  public void set_NTWK_ID(String NTWK_ID) {
    this.NTWK_ID = NTWK_ID;
  }
  public QueryResult with_NTWK_ID(String NTWK_ID) {
    this.NTWK_ID = NTWK_ID;
    return this;
  }
  private String CLM_NTWK_KEY;
  public String get_CLM_NTWK_KEY() {
    return CLM_NTWK_KEY;
  }
  public void set_CLM_NTWK_KEY(String CLM_NTWK_KEY) {
    this.CLM_NTWK_KEY = CLM_NTWK_KEY;
  }
  public QueryResult with_CLM_NTWK_KEY(String CLM_NTWK_KEY) {
    this.CLM_NTWK_KEY = CLM_NTWK_KEY;
    return this;
  }
  private String OBS_Flag;
  public String get_OBS_Flag() {
    return OBS_Flag;
  }
  public void set_OBS_Flag(String OBS_Flag) {
    this.OBS_Flag = OBS_Flag;
  }
  public QueryResult with_OBS_Flag(String OBS_Flag) {
    this.OBS_Flag = OBS_Flag;
    return this;
  }
  private String OUT_ITEM_STTS_IND_TXT;
  public String get_OUT_ITEM_STTS_IND_TXT() {
    return OUT_ITEM_STTS_IND_TXT;
  }
  public void set_OUT_ITEM_STTS_IND_TXT(String OUT_ITEM_STTS_IND_TXT) {
    this.OUT_ITEM_STTS_IND_TXT = OUT_ITEM_STTS_IND_TXT;
  }
  public QueryResult with_OUT_ITEM_STTS_IND_TXT(String OUT_ITEM_STTS_IND_TXT) {
    this.OUT_ITEM_STTS_IND_TXT = OUT_ITEM_STTS_IND_TXT;
    return this;
  }
  private Double PAID_AMT;
  public Double get_PAID_AMT() {
    return PAID_AMT;
  }
  public void set_PAID_AMT(Double PAID_AMT) {
    this.PAID_AMT = PAID_AMT;
  }
  public QueryResult with_PAID_AMT(Double PAID_AMT) {
    this.PAID_AMT = PAID_AMT;
    return this;
  }
  private java.math.BigDecimal PAID_SRVC_UNIT_CNT;
  public java.math.BigDecimal get_PAID_SRVC_UNIT_CNT() {
    return PAID_SRVC_UNIT_CNT;
  }
  public void set_PAID_SRVC_UNIT_CNT(java.math.BigDecimal PAID_SRVC_UNIT_CNT) {
    this.PAID_SRVC_UNIT_CNT = PAID_SRVC_UNIT_CNT;
  }
  public QueryResult with_PAID_SRVC_UNIT_CNT(java.math.BigDecimal PAID_SRVC_UNIT_CNT) {
    this.PAID_SRVC_UNIT_CNT = PAID_SRVC_UNIT_CNT;
    return this;
  }
  private String PARG_STTS_CD;
  public String get_PARG_STTS_CD() {
    return PARG_STTS_CD;
  }
  public void set_PARG_STTS_CD(String PARG_STTS_CD) {
    this.PARG_STTS_CD = PARG_STTS_CD;
  }
  public QueryResult with_PARG_STTS_CD(String PARG_STTS_CD) {
    this.PARG_STTS_CD = PARG_STTS_CD;
    return this;
  }
  private String PAYMNT_APC_NBR;
  public String get_PAYMNT_APC_NBR() {
    return PAYMNT_APC_NBR;
  }
  public void set_PAYMNT_APC_NBR(String PAYMNT_APC_NBR) {
    this.PAYMNT_APC_NBR = PAYMNT_APC_NBR;
  }
  public QueryResult with_PAYMNT_APC_NBR(String PAYMNT_APC_NBR) {
    this.PAYMNT_APC_NBR = PAYMNT_APC_NBR;
    return this;
  }
  private String PN_ID;
  public String get_PN_ID() {
    return PN_ID;
  }
  public void set_PN_ID(String PN_ID) {
    this.PN_ID = PN_ID;
  }
  public QueryResult with_PN_ID(String PN_ID) {
    this.PN_ID = PN_ID;
    return this;
  }
  private String PROD_CF_CD;
  public String get_PROD_CF_CD() {
    return PROD_CF_CD;
  }
  public void set_PROD_CF_CD(String PROD_CF_CD) {
    this.PROD_CF_CD = PROD_CF_CD;
  }
  public QueryResult with_PROD_CF_CD(String PROD_CF_CD) {
    this.PROD_CF_CD = PROD_CF_CD;
    return this;
  }
  private String PROD_ID;
  public String get_PROD_ID() {
    return PROD_ID;
  }
  public void set_PROD_ID(String PROD_ID) {
    this.PROD_ID = PROD_ID;
  }
  public QueryResult with_PROD_ID(String PROD_ID) {
    this.PROD_ID = PROD_ID;
    return this;
  }
  private String PROV_CITY_NM;
  public String get_PROV_CITY_NM() {
    return PROV_CITY_NM;
  }
  public void set_PROV_CITY_NM(String PROV_CITY_NM) {
    this.PROV_CITY_NM = PROV_CITY_NM;
  }
  public QueryResult with_PROV_CITY_NM(String PROV_CITY_NM) {
    this.PROV_CITY_NM = PROV_CITY_NM;
    return this;
  }
  private String PROV_ID;
  public String get_PROV_ID() {
    return PROV_ID;
  }
  public void set_PROV_ID(String PROV_ID) {
    this.PROV_ID = PROV_ID;
  }
  public QueryResult with_PROV_ID(String PROV_ID) {
    this.PROV_ID = PROV_ID;
    return this;
  }
  private String PROV_NM;
  public String get_PROV_NM() {
    return PROV_NM;
  }
  public void set_PROV_NM(String PROV_NM) {
    this.PROV_NM = PROV_NM;
  }
  public QueryResult with_PROV_NM(String PROV_NM) {
    this.PROV_NM = PROV_NM;
    return this;
  }
  private String PROV_ST_NM;
  public String get_PROV_ST_NM() {
    return PROV_ST_NM;
  }
  public void set_PROV_ST_NM(String PROV_ST_NM) {
    this.PROV_ST_NM = PROV_ST_NM;
  }
  public QueryResult with_PROV_ST_NM(String PROV_ST_NM) {
    this.PROV_ST_NM = PROV_ST_NM;
    return this;
  }
  private String PROV_ZIP_CD;
  public String get_PROV_ZIP_CD() {
    return PROV_ZIP_CD;
  }
  public void set_PROV_ZIP_CD(String PROV_ZIP_CD) {
    this.PROV_ZIP_CD = PROV_ZIP_CD;
  }
  public QueryResult with_PROV_ZIP_CD(String PROV_ZIP_CD) {
    this.PROV_ZIP_CD = PROV_ZIP_CD;
    return this;
  }
  private String RDCTN_CTGRY_CD;
  public String get_RDCTN_CTGRY_CD() {
    return RDCTN_CTGRY_CD;
  }
  public void set_RDCTN_CTGRY_CD(String RDCTN_CTGRY_CD) {
    this.RDCTN_CTGRY_CD = RDCTN_CTGRY_CD;
  }
  public QueryResult with_RDCTN_CTGRY_CD(String RDCTN_CTGRY_CD) {
    this.RDCTN_CTGRY_CD = RDCTN_CTGRY_CD;
    return this;
  }
  private Double RELATIVE_WEIGHT;
  public Double get_RELATIVE_WEIGHT() {
    return RELATIVE_WEIGHT;
  }
  public void set_RELATIVE_WEIGHT(Double RELATIVE_WEIGHT) {
    this.RELATIVE_WEIGHT = RELATIVE_WEIGHT;
  }
  public QueryResult with_RELATIVE_WEIGHT(Double RELATIVE_WEIGHT) {
    this.RELATIVE_WEIGHT = RELATIVE_WEIGHT;
    return this;
  }
  private Double RELATIVE_WEIGHT1;
  public Double get_RELATIVE_WEIGHT1() {
    return RELATIVE_WEIGHT1;
  }
  public void set_RELATIVE_WEIGHT1(Double RELATIVE_WEIGHT1) {
    this.RELATIVE_WEIGHT1 = RELATIVE_WEIGHT1;
  }
  public QueryResult with_RELATIVE_WEIGHT1(Double RELATIVE_WEIGHT1) {
    this.RELATIVE_WEIGHT1 = RELATIVE_WEIGHT1;
    return this;
  }
  private String RNDRG_PROV_ID;
  public String get_RNDRG_PROV_ID() {
    return RNDRG_PROV_ID;
  }
  public void set_RNDRG_PROV_ID(String RNDRG_PROV_ID) {
    this.RNDRG_PROV_ID = RNDRG_PROV_ID;
  }
  public QueryResult with_RNDRG_PROV_ID(String RNDRG_PROV_ID) {
    this.RNDRG_PROV_ID = RNDRG_PROV_ID;
    return this;
  }
  private String RNDRG_PROV_ID_TYPE_CD;
  public String get_RNDRG_PROV_ID_TYPE_CD() {
    return RNDRG_PROV_ID_TYPE_CD;
  }
  public void set_RNDRG_PROV_ID_TYPE_CD(String RNDRG_PROV_ID_TYPE_CD) {
    this.RNDRG_PROV_ID_TYPE_CD = RNDRG_PROV_ID_TYPE_CD;
  }
  public QueryResult with_RNDRG_PROV_ID_TYPE_CD(String RNDRG_PROV_ID_TYPE_CD) {
    this.RNDRG_PROV_ID_TYPE_CD = RNDRG_PROV_ID_TYPE_CD;
    return this;
  }
  private String RVNU_CD;
  public String get_RVNU_CD() {
    return RVNU_CD;
  }
  public void set_RVNU_CD(String RVNU_CD) {
    this.RVNU_CD = RVNU_CD;
  }
  public QueryResult with_RVNU_CD(String RVNU_CD) {
    this.RVNU_CD = RVNU_CD;
    return this;
  }
  private String RVU_Flag;
  public String get_RVU_Flag() {
    return RVU_Flag;
  }
  public void set_RVU_Flag(String RVU_Flag) {
    this.RVU_Flag = RVU_Flag;
  }
  public QueryResult with_RVU_Flag(String RVU_Flag) {
    this.RVU_Flag = RVU_Flag;
    return this;
  }
  private String SRC_APRVL_CD;
  public String get_SRC_APRVL_CD() {
    return SRC_APRVL_CD;
  }
  public void set_SRC_APRVL_CD(String SRC_APRVL_CD) {
    this.SRC_APRVL_CD = SRC_APRVL_CD;
  }
  public QueryResult with_SRC_APRVL_CD(String SRC_APRVL_CD) {
    this.SRC_APRVL_CD = SRC_APRVL_CD;
    return this;
  }
  private String SRC_BILLG_TAX_ID;
  public String get_SRC_BILLG_TAX_ID() {
    return SRC_BILLG_TAX_ID;
  }
  public void set_SRC_BILLG_TAX_ID(String SRC_BILLG_TAX_ID) {
    this.SRC_BILLG_TAX_ID = SRC_BILLG_TAX_ID;
  }
  public QueryResult with_SRC_BILLG_TAX_ID(String SRC_BILLG_TAX_ID) {
    this.SRC_BILLG_TAX_ID = SRC_BILLG_TAX_ID;
    return this;
  }
  private String SRC_CLM_DISP_CD;
  public String get_SRC_CLM_DISP_CD() {
    return SRC_CLM_DISP_CD;
  }
  public void set_SRC_CLM_DISP_CD(String SRC_CLM_DISP_CD) {
    this.SRC_CLM_DISP_CD = SRC_CLM_DISP_CD;
  }
  public QueryResult with_SRC_CLM_DISP_CD(String SRC_CLM_DISP_CD) {
    this.SRC_CLM_DISP_CD = SRC_CLM_DISP_CD;
    return this;
  }
  private java.math.BigDecimal SRC_FINDER_NBR;
  public java.math.BigDecimal get_SRC_FINDER_NBR() {
    return SRC_FINDER_NBR;
  }
  public void set_SRC_FINDER_NBR(java.math.BigDecimal SRC_FINDER_NBR) {
    this.SRC_FINDER_NBR = SRC_FINDER_NBR;
  }
  public QueryResult with_SRC_FINDER_NBR(java.math.BigDecimal SRC_FINDER_NBR) {
    this.SRC_FINDER_NBR = SRC_FINDER_NBR;
    return this;
  }
  private String SRC_GRP_NBR;
  public String get_SRC_GRP_NBR() {
    return SRC_GRP_NBR;
  }
  public void set_SRC_GRP_NBR(String SRC_GRP_NBR) {
    this.SRC_GRP_NBR = SRC_GRP_NBR;
  }
  public QueryResult with_SRC_GRP_NBR(String SRC_GRP_NBR) {
    this.SRC_GRP_NBR = SRC_GRP_NBR;
    return this;
  }
  private String SRC_HMO_CLS_CD;
  public String get_SRC_HMO_CLS_CD() {
    return SRC_HMO_CLS_CD;
  }
  public void set_SRC_HMO_CLS_CD(String SRC_HMO_CLS_CD) {
    this.SRC_HMO_CLS_CD = SRC_HMO_CLS_CD;
  }
  public QueryResult with_SRC_HMO_CLS_CD(String SRC_HMO_CLS_CD) {
    this.SRC_HMO_CLS_CD = SRC_HMO_CLS_CD;
    return this;
  }
  private String SRC_INSTNL_NGTTD_SRVC_TERM_ID;
  public String get_SRC_INSTNL_NGTTD_SRVC_TERM_ID() {
    return SRC_INSTNL_NGTTD_SRVC_TERM_ID;
  }
  public void set_SRC_INSTNL_NGTTD_SRVC_TERM_ID(String SRC_INSTNL_NGTTD_SRVC_TERM_ID) {
    this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = SRC_INSTNL_NGTTD_SRVC_TERM_ID;
  }
  public QueryResult with_SRC_INSTNL_NGTTD_SRVC_TERM_ID(String SRC_INSTNL_NGTTD_SRVC_TERM_ID) {
    this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = SRC_INSTNL_NGTTD_SRVC_TERM_ID;
    return this;
  }
  private String SRC_INSTNL_REIMBMNT_TERM_ID;
  public String get_SRC_INSTNL_REIMBMNT_TERM_ID() {
    return SRC_INSTNL_REIMBMNT_TERM_ID;
  }
  public void set_SRC_INSTNL_REIMBMNT_TERM_ID(String SRC_INSTNL_REIMBMNT_TERM_ID) {
    this.SRC_INSTNL_REIMBMNT_TERM_ID = SRC_INSTNL_REIMBMNT_TERM_ID;
  }
  public QueryResult with_SRC_INSTNL_REIMBMNT_TERM_ID(String SRC_INSTNL_REIMBMNT_TERM_ID) {
    this.SRC_INSTNL_REIMBMNT_TERM_ID = SRC_INSTNL_REIMBMNT_TERM_ID;
    return this;
  }
  private String SRC_MEDCR_ID;
  public String get_SRC_MEDCR_ID() {
    return SRC_MEDCR_ID;
  }
  public void set_SRC_MEDCR_ID(String SRC_MEDCR_ID) {
    this.SRC_MEDCR_ID = SRC_MEDCR_ID;
  }
  public QueryResult with_SRC_MEDCR_ID(String SRC_MEDCR_ID) {
    this.SRC_MEDCR_ID = SRC_MEDCR_ID;
    return this;
  }
  private String SRC_NST_SRVC_CTGRY_CD;
  public String get_SRC_NST_SRVC_CTGRY_CD() {
    return SRC_NST_SRVC_CTGRY_CD;
  }
  public void set_SRC_NST_SRVC_CTGRY_CD(String SRC_NST_SRVC_CTGRY_CD) {
    this.SRC_NST_SRVC_CTGRY_CD = SRC_NST_SRVC_CTGRY_CD;
  }
  public QueryResult with_SRC_NST_SRVC_CTGRY_CD(String SRC_NST_SRVC_CTGRY_CD) {
    this.SRC_NST_SRVC_CTGRY_CD = SRC_NST_SRVC_CTGRY_CD;
    return this;
  }
  private String SRC_PAY_ACTN_CD;
  public String get_SRC_PAY_ACTN_CD() {
    return SRC_PAY_ACTN_CD;
  }
  public void set_SRC_PAY_ACTN_CD(String SRC_PAY_ACTN_CD) {
    this.SRC_PAY_ACTN_CD = SRC_PAY_ACTN_CD;
  }
  public QueryResult with_SRC_PAY_ACTN_CD(String SRC_PAY_ACTN_CD) {
    this.SRC_PAY_ACTN_CD = SRC_PAY_ACTN_CD;
    return this;
  }
  private String SRC_PRCG_CRTRIA_CD;
  public String get_SRC_PRCG_CRTRIA_CD() {
    return SRC_PRCG_CRTRIA_CD;
  }
  public void set_SRC_PRCG_CRTRIA_CD(String SRC_PRCG_CRTRIA_CD) {
    this.SRC_PRCG_CRTRIA_CD = SRC_PRCG_CRTRIA_CD;
  }
  public QueryResult with_SRC_PRCG_CRTRIA_CD(String SRC_PRCG_CRTRIA_CD) {
    this.SRC_PRCG_CRTRIA_CD = SRC_PRCG_CRTRIA_CD;
    return this;
  }
  private String SRC_PRCG_RSN_CD;
  public String get_SRC_PRCG_RSN_CD() {
    return SRC_PRCG_RSN_CD;
  }
  public void set_SRC_PRCG_RSN_CD(String SRC_PRCG_RSN_CD) {
    this.SRC_PRCG_RSN_CD = SRC_PRCG_RSN_CD;
  }
  public QueryResult with_SRC_PRCG_RSN_CD(String SRC_PRCG_RSN_CD) {
    this.SRC_PRCG_RSN_CD = SRC_PRCG_RSN_CD;
    return this;
  }
  private String SRC_PROV_NATL_PROV_ID;
  public String get_SRC_PROV_NATL_PROV_ID() {
    return SRC_PROV_NATL_PROV_ID;
  }
  public void set_SRC_PROV_NATL_PROV_ID(String SRC_PROV_NATL_PROV_ID) {
    this.SRC_PROV_NATL_PROV_ID = SRC_PROV_NATL_PROV_ID;
  }
  public QueryResult with_SRC_PROV_NATL_PROV_ID(String SRC_PROV_NATL_PROV_ID) {
    this.SRC_PROV_NATL_PROV_ID = SRC_PROV_NATL_PROV_ID;
    return this;
  }
  private java.math.BigDecimal SRC_PRTY_SCOR_NBR;
  public java.math.BigDecimal get_SRC_PRTY_SCOR_NBR() {
    return SRC_PRTY_SCOR_NBR;
  }
  public void set_SRC_PRTY_SCOR_NBR(java.math.BigDecimal SRC_PRTY_SCOR_NBR) {
    this.SRC_PRTY_SCOR_NBR = SRC_PRTY_SCOR_NBR;
  }
  public QueryResult with_SRC_PRTY_SCOR_NBR(java.math.BigDecimal SRC_PRTY_SCOR_NBR) {
    this.SRC_PRTY_SCOR_NBR = SRC_PRTY_SCOR_NBR;
    return this;
  }
  private String SRC_SRVC_CLSFCTN_CD;
  public String get_SRC_SRVC_CLSFCTN_CD() {
    return SRC_SRVC_CLSFCTN_CD;
  }
  public void set_SRC_SRVC_CLSFCTN_CD(String SRC_SRVC_CLSFCTN_CD) {
    this.SRC_SRVC_CLSFCTN_CD = SRC_SRVC_CLSFCTN_CD;
  }
  public QueryResult with_SRC_SRVC_CLSFCTN_CD(String SRC_SRVC_CLSFCTN_CD) {
    this.SRC_SRVC_CLSFCTN_CD = SRC_SRVC_CLSFCTN_CD;
    return this;
  }
  private java.sql.Date SRC_TERM_EFCTV_DT;
  public java.sql.Date get_SRC_TERM_EFCTV_DT() {
    return SRC_TERM_EFCTV_DT;
  }
  public void set_SRC_TERM_EFCTV_DT(java.sql.Date SRC_TERM_EFCTV_DT) {
    this.SRC_TERM_EFCTV_DT = SRC_TERM_EFCTV_DT;
  }
  public QueryResult with_SRC_TERM_EFCTV_DT(java.sql.Date SRC_TERM_EFCTV_DT) {
    this.SRC_TERM_EFCTV_DT = SRC_TERM_EFCTV_DT;
    return this;
  }
  private java.sql.Date SRC_TERM_END_DT;
  public java.sql.Date get_SRC_TERM_END_DT() {
    return SRC_TERM_END_DT;
  }
  public void set_SRC_TERM_END_DT(java.sql.Date SRC_TERM_END_DT) {
    this.SRC_TERM_END_DT = SRC_TERM_END_DT;
  }
  public QueryResult with_SRC_TERM_END_DT(java.sql.Date SRC_TERM_END_DT) {
    this.SRC_TERM_END_DT = SRC_TERM_END_DT;
    return this;
  }
  private String TOS_CD;
  public String get_TOS_CD() {
    return TOS_CD;
  }
  public void set_TOS_CD(String TOS_CD) {
    this.TOS_CD = TOS_CD;
  }
  public QueryResult with_TOS_CD(String TOS_CD) {
    this.TOS_CD = TOS_CD;
    return this;
  }
  private String apcst;
  public String get_apcst() {
    return apcst;
  }
  public void set_apcst(String apcst) {
    this.apcst = apcst;
  }
  public QueryResult with_apcst(String apcst) {
    this.apcst = apcst;
    return this;
  }
  private Double apcwt;
  public Double get_apcwt() {
    return apcwt;
  }
  public void set_apcwt(Double apcwt) {
    this.apcwt = apcwt;
  }
  public QueryResult with_apcwt(Double apcwt) {
    this.apcwt = apcwt;
    return this;
  }
  private String betos;
  public String get_betos() {
    return betos;
  }
  public void set_betos(String betos) {
    this.betos = betos;
  }
  public QueryResult with_betos(String betos) {
    this.betos = betos;
    return this;
  }
  private String brand;
  public String get_brand() {
    return brand;
  }
  public void set_brand(String brand) {
    this.brand = brand;
  }
  public QueryResult with_brand(String brand) {
    this.brand = brand;
    return this;
  }
  private String cat1;
  public String get_cat1() {
    return cat1;
  }
  public void set_cat1(String cat1) {
    this.cat1 = cat1;
  }
  public QueryResult with_cat1(String cat1) {
    this.cat1 = cat1;
    return this;
  }
  private String cat2;
  public String get_cat2() {
    return cat2;
  }
  public void set_cat2(String cat2) {
    this.cat2 = cat2;
  }
  public QueryResult with_cat2(String cat2) {
    this.cat2 = cat2;
    return this;
  }
  private Double clmwt;
  public Double get_clmwt() {
    return clmwt;
  }
  public void set_clmwt(Double clmwt) {
    this.clmwt = clmwt;
  }
  public QueryResult with_clmwt(Double clmwt) {
    this.clmwt = clmwt;
    return this;
  }
  private Double erwt;
  public Double get_erwt() {
    return erwt;
  }
  public void set_erwt(Double erwt) {
    this.erwt = erwt;
  }
  public QueryResult with_erwt(Double erwt) {
    this.erwt = erwt;
    return this;
  }
  private String fundlvl2;
  public String get_fundlvl2() {
    return fundlvl2;
  }
  public void set_fundlvl2(String fundlvl2) {
    this.fundlvl2 = fundlvl2;
  }
  public QueryResult with_fundlvl2(String fundlvl2) {
    this.fundlvl2 = fundlvl2;
    return this;
  }
  private String liccd;
  public String get_liccd() {
    return liccd;
  }
  public void set_liccd(String liccd) {
    this.liccd = liccd;
  }
  public QueryResult with_liccd(String liccd) {
    this.liccd = liccd;
    return this;
  }
  private String market;
  public String get_market() {
    return market;
  }
  public void set_market(String market) {
    this.market = market;
  }
  public QueryResult with_market(String market) {
    this.market = market;
    return this;
  }
  private String Inc_Month;
  public String get_Inc_Month() {
    return Inc_Month;
  }
  public void set_Inc_Month(String Inc_Month) {
    this.Inc_Month = Inc_Month;
  }
  public QueryResult with_Inc_Month(String Inc_Month) {
    this.Inc_Month = Inc_Month;
    return this;
  }
  private String network;
  public String get_network() {
    return network;
  }
  public void set_network(String network) {
    this.network = network;
  }
  public QueryResult with_network(String network) {
    this.network = network;
    return this;
  }
  private String normflag;
  public String get_normflag() {
    return normflag;
  }
  public void set_normflag(String normflag) {
    this.normflag = normflag;
  }
  public QueryResult with_normflag(String normflag) {
    this.normflag = normflag;
    return this;
  }
  private String priority;
  public String get_priority() {
    return priority;
  }
  public void set_priority(String priority) {
    this.priority = priority;
  }
  public QueryResult with_priority(String priority) {
    this.priority = priority;
    return this;
  }
  private String prodlvl3;
  public String get_prodlvl3() {
    return prodlvl3;
  }
  public void set_prodlvl3(String prodlvl3) {
    this.prodlvl3 = prodlvl3;
  }
  public QueryResult with_prodlvl3(String prodlvl3) {
    this.prodlvl3 = prodlvl3;
    return this;
  }
  private String prov_county;
  public String get_prov_county() {
    return prov_county;
  }
  public void set_prov_county(String prov_county) {
    this.prov_county = prov_county;
  }
  public QueryResult with_prov_county(String prov_county) {
    this.prov_county = prov_county;
    return this;
  }
  private String surgrp;
  public String get_surgrp() {
    return surgrp;
  }
  public void set_surgrp(String surgrp) {
    this.surgrp = surgrp;
  }
  public QueryResult with_surgrp(String surgrp) {
    this.surgrp = surgrp;
    return this;
  }
  private Double surgwt;
  public Double get_surgwt() {
    return surgwt;
  }
  public void set_surgwt(Double surgwt) {
    this.surgwt = surgwt;
  }
  public QueryResult with_surgwt(Double surgwt) {
    this.surgwt = surgwt;
    return this;
  }
  private String system_id;
  public String get_system_id() {
    return system_id;
  }
  public void set_system_id(String system_id) {
    this.system_id = system_id;
  }
  public QueryResult with_system_id(String system_id) {
    this.system_id = system_id;
    return this;
  }
  private String GRP_NM;
  public String get_GRP_NM() {
    return GRP_NM;
  }
  public void set_GRP_NM(String GRP_NM) {
    this.GRP_NM = GRP_NM;
  }
  public QueryResult with_GRP_NM(String GRP_NM) {
    this.GRP_NM = GRP_NM;
    return this;
  }
  private String SRC_SUBGRP_NBR;
  public String get_SRC_SUBGRP_NBR() {
    return SRC_SUBGRP_NBR;
  }
  public void set_SRC_SUBGRP_NBR(String SRC_SUBGRP_NBR) {
    this.SRC_SUBGRP_NBR = SRC_SUBGRP_NBR;
  }
  public QueryResult with_SRC_SUBGRP_NBR(String SRC_SUBGRP_NBR) {
    this.SRC_SUBGRP_NBR = SRC_SUBGRP_NBR;
    return this;
  }
  private Double VISITS;
  public Double get_VISITS() {
    return VISITS;
  }
  public void set_VISITS(Double VISITS) {
    this.VISITS = VISITS;
  }
  public QueryResult with_VISITS(Double VISITS) {
    this.VISITS = VISITS;
    return this;
  }
  private String GRP_SIC;
  public String get_GRP_SIC() {
    return GRP_SIC;
  }
  public void set_GRP_SIC(String GRP_SIC) {
    this.GRP_SIC = GRP_SIC;
  }
  public QueryResult with_GRP_SIC(String GRP_SIC) {
    this.GRP_SIC = GRP_SIC;
    return this;
  }
  private String MCID;
  public String get_MCID() {
    return MCID;
  }
  public void set_MCID(String MCID) {
    this.MCID = MCID;
  }
  public QueryResult with_MCID(String MCID) {
    this.MCID = MCID;
    return this;
  }
  private String CLM_ITS_SCCF_NBR;
  public String get_CLM_ITS_SCCF_NBR() {
    return CLM_ITS_SCCF_NBR;
  }
  public void set_CLM_ITS_SCCF_NBR(String CLM_ITS_SCCF_NBR) {
    this.CLM_ITS_SCCF_NBR = CLM_ITS_SCCF_NBR;
  }
  public QueryResult with_CLM_ITS_SCCF_NBR(String CLM_ITS_SCCF_NBR) {
    this.CLM_ITS_SCCF_NBR = CLM_ITS_SCCF_NBR;
    return this;
  }
  private String MBRSHP_SOR_CD;
  public String get_MBRSHP_SOR_CD() {
    return MBRSHP_SOR_CD;
  }
  public void set_MBRSHP_SOR_CD(String MBRSHP_SOR_CD) {
    this.MBRSHP_SOR_CD = MBRSHP_SOR_CD;
  }
  public QueryResult with_MBRSHP_SOR_CD(String MBRSHP_SOR_CD) {
    this.MBRSHP_SOR_CD = MBRSHP_SOR_CD;
    return this;
  }
  private String SRC_RT_CTGRY_CD;
  public String get_SRC_RT_CTGRY_CD() {
    return SRC_RT_CTGRY_CD;
  }
  public void set_SRC_RT_CTGRY_CD(String SRC_RT_CTGRY_CD) {
    this.SRC_RT_CTGRY_CD = SRC_RT_CTGRY_CD;
  }
  public QueryResult with_SRC_RT_CTGRY_CD(String SRC_RT_CTGRY_CD) {
    this.SRC_RT_CTGRY_CD = SRC_RT_CTGRY_CD;
    return this;
  }
  private String SRC_RT_CTGRY_SUB_CD;
  public String get_SRC_RT_CTGRY_SUB_CD() {
    return SRC_RT_CTGRY_SUB_CD;
  }
  public void set_SRC_RT_CTGRY_SUB_CD(String SRC_RT_CTGRY_SUB_CD) {
    this.SRC_RT_CTGRY_SUB_CD = SRC_RT_CTGRY_SUB_CD;
  }
  public QueryResult with_SRC_RT_CTGRY_SUB_CD(String SRC_RT_CTGRY_SUB_CD) {
    this.SRC_RT_CTGRY_SUB_CD = SRC_RT_CTGRY_SUB_CD;
    return this;
  }
  private java.sql.Date run_date;
  public java.sql.Date get_run_date() {
    return run_date;
  }
  public void set_run_date(java.sql.Date run_date) {
    this.run_date = run_date;
  }
  public QueryResult with_run_date(java.sql.Date run_date) {
    this.run_date = run_date;
    return this;
  }
  private java.sql.Date CLM_LINE_SRVC_STRT_DT;
  public java.sql.Date get_CLM_LINE_SRVC_STRT_DT() {
    return CLM_LINE_SRVC_STRT_DT;
  }
  public void set_CLM_LINE_SRVC_STRT_DT(java.sql.Date CLM_LINE_SRVC_STRT_DT) {
    this.CLM_LINE_SRVC_STRT_DT = CLM_LINE_SRVC_STRT_DT;
  }
  public QueryResult with_CLM_LINE_SRVC_STRT_DT(java.sql.Date CLM_LINE_SRVC_STRT_DT) {
    this.CLM_LINE_SRVC_STRT_DT = CLM_LINE_SRVC_STRT_DT;
    return this;
  }
  private String VNDR_CD;
  public String get_VNDR_CD() {
    return VNDR_CD;
  }
  public void set_VNDR_CD(String VNDR_CD) {
    this.VNDR_CD = VNDR_CD;
  }
  public QueryResult with_VNDR_CD(String VNDR_CD) {
    this.VNDR_CD = VNDR_CD;
    return this;
  }
  private String REPRICE_CD;
  public String get_REPRICE_CD() {
    return REPRICE_CD;
  }
  public void set_REPRICE_CD(String REPRICE_CD) {
    this.REPRICE_CD = REPRICE_CD;
  }
  public QueryResult with_REPRICE_CD(String REPRICE_CD) {
    this.REPRICE_CD = REPRICE_CD;
    return this;
  }
  private String VNDR_PROD_CD;
  public String get_VNDR_PROD_CD() {
    return VNDR_PROD_CD;
  }
  public void set_VNDR_PROD_CD(String VNDR_PROD_CD) {
    this.VNDR_PROD_CD = VNDR_PROD_CD;
  }
  public QueryResult with_VNDR_PROD_CD(String VNDR_PROD_CD) {
    this.VNDR_PROD_CD = VNDR_PROD_CD;
    return this;
  }
  private String zip3;
  public String get_zip3() {
    return zip3;
  }
  public void set_zip3(String zip3) {
    this.zip3 = zip3;
  }
  public QueryResult with_zip3(String zip3) {
    this.zip3 = zip3;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.ADJDCTN_DT == null ? that.ADJDCTN_DT == null : this.ADJDCTN_DT.equals(that.ADJDCTN_DT));
    equal = equal && (this.ADJDCTN_STTS == null ? that.ADJDCTN_STTS == null : this.ADJDCTN_STTS.equals(that.ADJDCTN_STTS));
    equal = equal && (this.ADMSN_TYPE_CD == null ? that.ADMSN_TYPE_CD == null : this.ADMSN_TYPE_CD.equals(that.ADMSN_TYPE_CD));
    equal = equal && (this.ADMT_DT == null ? that.ADMT_DT == null : this.ADMT_DT.equals(that.ADMT_DT));
    equal = equal && (this.ALWD_AMT == null ? that.ALWD_AMT == null : this.ALWD_AMT.equals(that.ALWD_AMT));
    equal = equal && (this.APC_MEDCR_ID == null ? that.APC_MEDCR_ID == null : this.APC_MEDCR_ID.equals(that.APC_MEDCR_ID));
    equal = equal && (this.APC_VRSN_NBR == null ? that.APC_VRSN_NBR == null : this.APC_VRSN_NBR.equals(that.APC_VRSN_NBR));
    equal = equal && (this.BED_TYPE_CD == null ? that.BED_TYPE_CD == null : this.BED_TYPE_CD.equals(that.BED_TYPE_CD));
    equal = equal && (this.BILLD_CHRG_AMT == null ? that.BILLD_CHRG_AMT == null : this.BILLD_CHRG_AMT.equals(that.BILLD_CHRG_AMT));
    equal = equal && (this.BILLD_SRVC_UNIT_CNT == null ? that.BILLD_SRVC_UNIT_CNT == null : this.BILLD_SRVC_UNIT_CNT.equals(that.BILLD_SRVC_UNIT_CNT));
    equal = equal && (this.BNFT_PAYMNT_STTS_CD == null ? that.BNFT_PAYMNT_STTS_CD == null : this.BNFT_PAYMNT_STTS_CD.equals(that.BNFT_PAYMNT_STTS_CD));
    equal = equal && (this.CASES == null ? that.CASES == null : this.CASES.equals(that.CASES));
    equal = equal && (this.CLM_ADJSTMNT_KEY == null ? that.CLM_ADJSTMNT_KEY == null : this.CLM_ADJSTMNT_KEY.equals(that.CLM_ADJSTMNT_KEY));
    equal = equal && (this.CLM_ADJSTMNT_NBR == null ? that.CLM_ADJSTMNT_NBR == null : this.CLM_ADJSTMNT_NBR.equals(that.CLM_ADJSTMNT_NBR));
    equal = equal && (this.CLM_DISP_CD == null ? that.CLM_DISP_CD == null : this.CLM_DISP_CD.equals(that.CLM_DISP_CD));
    equal = equal && (this.CLM_ITS_HOST_CD == null ? that.CLM_ITS_HOST_CD == null : this.CLM_ITS_HOST_CD.equals(that.CLM_ITS_HOST_CD));
    equal = equal && (this.CLM_LINE_NBR == null ? that.CLM_LINE_NBR == null : this.CLM_LINE_NBR.equals(that.CLM_LINE_NBR));
    equal = equal && (this.CLM_LINE_REIMB_MTHD_CD == null ? that.CLM_LINE_REIMB_MTHD_CD == null : this.CLM_LINE_REIMB_MTHD_CD.equals(that.CLM_LINE_REIMB_MTHD_CD));
    equal = equal && (this.CLM_NBR == null ? that.CLM_NBR == null : this.CLM_NBR.equals(that.CLM_NBR));
    equal = equal && (this.CLM_REIMBMNT_SOR_CD == null ? that.CLM_REIMBMNT_SOR_CD == null : this.CLM_REIMBMNT_SOR_CD.equals(that.CLM_REIMBMNT_SOR_CD));
    equal = equal && (this.CLM_REIMBMNT_TYPE_CD == null ? that.CLM_REIMBMNT_TYPE_CD == null : this.CLM_REIMBMNT_TYPE_CD.equals(that.CLM_REIMBMNT_TYPE_CD));
    equal = equal && (this.CLM_SOR_CD == null ? that.CLM_SOR_CD == null : this.CLM_SOR_CD.equals(that.CLM_SOR_CD));
    equal = equal && (this.CLM_STMNT_FROM_DT == null ? that.CLM_STMNT_FROM_DT == null : this.CLM_STMNT_FROM_DT.equals(that.CLM_STMNT_FROM_DT));
    equal = equal && (this.CLM_STMNT_TO_DT == null ? that.CLM_STMNT_TO_DT == null : this.CLM_STMNT_TO_DT.equals(that.CLM_STMNT_TO_DT));
    equal = equal && (this.CMAD == null ? that.CMAD == null : this.CMAD.equals(that.CMAD));
    equal = equal && (this.CMAD1 == null ? that.CMAD1 == null : this.CMAD1.equals(that.CMAD1));
    equal = equal && (this.CMAD_ALLOWED == null ? that.CMAD_ALLOWED == null : this.CMAD_ALLOWED.equals(that.CMAD_ALLOWED));
    equal = equal && (this.CMAD_BILLED == null ? that.CMAD_BILLED == null : this.CMAD_BILLED.equals(that.CMAD_BILLED));
    equal = equal && (this.CMAD_CASES == null ? that.CMAD_CASES == null : this.CMAD_CASES.equals(that.CMAD_CASES));
    equal = equal && (this.CMPNY_CF_CD == null ? that.CMPNY_CF_CD == null : this.CMPNY_CF_CD.equals(that.CMPNY_CF_CD));
    equal = equal && (this.CVRD_EXPNS_AMT == null ? that.CVRD_EXPNS_AMT == null : this.CVRD_EXPNS_AMT.equals(that.CVRD_EXPNS_AMT));
    equal = equal && (this.DERIVD_IND_CD == null ? that.DERIVD_IND_CD == null : this.DERIVD_IND_CD.equals(that.DERIVD_IND_CD));
    equal = equal && (this.DSCHRG_DT == null ? that.DSCHRG_DT == null : this.DSCHRG_DT.equals(that.DSCHRG_DT));
    equal = equal && (this.DSCHRG_STTS_CD == null ? that.DSCHRG_STTS_CD == null : this.DSCHRG_STTS_CD.equals(that.DSCHRG_STTS_CD));
    equal = equal && (this.ER_Flag == null ? that.ER_Flag == null : this.ER_Flag.equals(that.ER_Flag));
    equal = equal && (this.PediatricFlag == null ? that.PediatricFlag == null : this.PediatricFlag.equals(that.PediatricFlag));
    equal = equal && (this.ENC_Flag == null ? that.ENC_Flag == null : this.ENC_Flag.equals(that.ENC_Flag));
    equal = equal && (this.EXCHNG_CERTFN_CD == null ? that.EXCHNG_CERTFN_CD == null : this.EXCHNG_CERTFN_CD.equals(that.EXCHNG_CERTFN_CD));
    equal = equal && (this.EXCHNG_IND_CD == null ? that.EXCHNG_IND_CD == null : this.EXCHNG_IND_CD.equals(that.EXCHNG_IND_CD));
    equal = equal && (this.EXCHNG_METAL_TYPE_CD == null ? that.EXCHNG_METAL_TYPE_CD == null : this.EXCHNG_METAL_TYPE_CD.equals(that.EXCHNG_METAL_TYPE_CD));
    equal = equal && (this.FUNDG_CF_CD == null ? that.FUNDG_CF_CD == null : this.FUNDG_CF_CD.equals(that.FUNDG_CF_CD));
    equal = equal && (this.HLTH_SRVC_CD == null ? that.HLTH_SRVC_CD == null : this.HLTH_SRVC_CD.equals(that.HLTH_SRVC_CD));
    equal = equal && (this.PROC_MDFR_1_CD == null ? that.PROC_MDFR_1_CD == null : this.PROC_MDFR_1_CD.equals(that.PROC_MDFR_1_CD));
    equal = equal && (this.PROC_MDFR_2_CD == null ? that.PROC_MDFR_2_CD == null : this.PROC_MDFR_2_CD.equals(that.PROC_MDFR_2_CD));
    equal = equal && (this.HMO_CLS_CD == null ? that.HMO_CLS_CD == null : this.HMO_CLS_CD.equals(that.HMO_CLS_CD));
    equal = equal && (this.ICL_ALWD_AMT == null ? that.ICL_ALWD_AMT == null : this.ICL_ALWD_AMT.equals(that.ICL_ALWD_AMT));
    equal = equal && (this.ICL_PAID_AMT == null ? that.ICL_PAID_AMT == null : this.ICL_PAID_AMT.equals(that.ICL_PAID_AMT));
    equal = equal && (this.INN_CD == null ? that.INN_CD == null : this.INN_CD.equals(that.INN_CD));
    equal = equal && (this.LEGACY == null ? that.LEGACY == null : this.LEGACY.equals(that.LEGACY));
    equal = equal && (this.LINE_ITEM_APC_WT_AMT == null ? that.LINE_ITEM_APC_WT_AMT == null : this.LINE_ITEM_APC_WT_AMT.equals(that.LINE_ITEM_APC_WT_AMT));
    equal = equal && (this.LINE_ITEM_PAYMNT_AMT == null ? that.LINE_ITEM_PAYMNT_AMT == null : this.LINE_ITEM_PAYMNT_AMT.equals(that.LINE_ITEM_PAYMNT_AMT));
    equal = equal && (this.MBR_County == null ? that.MBR_County == null : this.MBR_County.equals(that.MBR_County));
    equal = equal && (this.MBR_KEY == null ? that.MBR_KEY == null : this.MBR_KEY.equals(that.MBR_KEY));
    equal = equal && (this.MBR_State == null ? that.MBR_State == null : this.MBR_State.equals(that.MBR_State));
    equal = equal && (this.MBR_ZIP3 == null ? that.MBR_ZIP3 == null : this.MBR_ZIP3.equals(that.MBR_ZIP3));
    equal = equal && (this.MBR_ZIP_CD == null ? that.MBR_ZIP_CD == null : this.MBR_ZIP_CD.equals(that.MBR_ZIP_CD));
    equal = equal && (this.MBU_CF_CD == null ? that.MBU_CF_CD == null : this.MBU_CF_CD.equals(that.MBU_CF_CD));
    equal = equal && (this.MBUlvl1 == null ? that.MBUlvl1 == null : this.MBUlvl1.equals(that.MBUlvl1));
    equal = equal && (this.MBUlvl2 == null ? that.MBUlvl2 == null : this.MBUlvl2.equals(that.MBUlvl2));
    equal = equal && (this.MBUlvl3 == null ? that.MBUlvl3 == null : this.MBUlvl3.equals(that.MBUlvl3));
    equal = equal && (this.MBUlvl4 == null ? that.MBUlvl4 == null : this.MBUlvl4.equals(that.MBUlvl4));
    equal = equal && (this.MCS == null ? that.MCS == null : this.MCS.equals(that.MCS));
    equal = equal && (this.MEDCR_ID == null ? that.MEDCR_ID == null : this.MEDCR_ID.equals(that.MEDCR_ID));
    equal = equal && (this.NRMLZD_AMT == null ? that.NRMLZD_AMT == null : this.NRMLZD_AMT.equals(that.NRMLZD_AMT));
    equal = equal && (this.NRMLZD_CASE == null ? that.NRMLZD_CASE == null : this.NRMLZD_CASE.equals(that.NRMLZD_CASE));
    equal = equal && (this.NTWK_ID == null ? that.NTWK_ID == null : this.NTWK_ID.equals(that.NTWK_ID));
    equal = equal && (this.CLM_NTWK_KEY == null ? that.CLM_NTWK_KEY == null : this.CLM_NTWK_KEY.equals(that.CLM_NTWK_KEY));
    equal = equal && (this.OBS_Flag == null ? that.OBS_Flag == null : this.OBS_Flag.equals(that.OBS_Flag));
    equal = equal && (this.OUT_ITEM_STTS_IND_TXT == null ? that.OUT_ITEM_STTS_IND_TXT == null : this.OUT_ITEM_STTS_IND_TXT.equals(that.OUT_ITEM_STTS_IND_TXT));
    equal = equal && (this.PAID_AMT == null ? that.PAID_AMT == null : this.PAID_AMT.equals(that.PAID_AMT));
    equal = equal && (this.PAID_SRVC_UNIT_CNT == null ? that.PAID_SRVC_UNIT_CNT == null : this.PAID_SRVC_UNIT_CNT.equals(that.PAID_SRVC_UNIT_CNT));
    equal = equal && (this.PARG_STTS_CD == null ? that.PARG_STTS_CD == null : this.PARG_STTS_CD.equals(that.PARG_STTS_CD));
    equal = equal && (this.PAYMNT_APC_NBR == null ? that.PAYMNT_APC_NBR == null : this.PAYMNT_APC_NBR.equals(that.PAYMNT_APC_NBR));
    equal = equal && (this.PN_ID == null ? that.PN_ID == null : this.PN_ID.equals(that.PN_ID));
    equal = equal && (this.PROD_CF_CD == null ? that.PROD_CF_CD == null : this.PROD_CF_CD.equals(that.PROD_CF_CD));
    equal = equal && (this.PROD_ID == null ? that.PROD_ID == null : this.PROD_ID.equals(that.PROD_ID));
    equal = equal && (this.PROV_CITY_NM == null ? that.PROV_CITY_NM == null : this.PROV_CITY_NM.equals(that.PROV_CITY_NM));
    equal = equal && (this.PROV_ID == null ? that.PROV_ID == null : this.PROV_ID.equals(that.PROV_ID));
    equal = equal && (this.PROV_NM == null ? that.PROV_NM == null : this.PROV_NM.equals(that.PROV_NM));
    equal = equal && (this.PROV_ST_NM == null ? that.PROV_ST_NM == null : this.PROV_ST_NM.equals(that.PROV_ST_NM));
    equal = equal && (this.PROV_ZIP_CD == null ? that.PROV_ZIP_CD == null : this.PROV_ZIP_CD.equals(that.PROV_ZIP_CD));
    equal = equal && (this.RDCTN_CTGRY_CD == null ? that.RDCTN_CTGRY_CD == null : this.RDCTN_CTGRY_CD.equals(that.RDCTN_CTGRY_CD));
    equal = equal && (this.RELATIVE_WEIGHT == null ? that.RELATIVE_WEIGHT == null : this.RELATIVE_WEIGHT.equals(that.RELATIVE_WEIGHT));
    equal = equal && (this.RELATIVE_WEIGHT1 == null ? that.RELATIVE_WEIGHT1 == null : this.RELATIVE_WEIGHT1.equals(that.RELATIVE_WEIGHT1));
    equal = equal && (this.RNDRG_PROV_ID == null ? that.RNDRG_PROV_ID == null : this.RNDRG_PROV_ID.equals(that.RNDRG_PROV_ID));
    equal = equal && (this.RNDRG_PROV_ID_TYPE_CD == null ? that.RNDRG_PROV_ID_TYPE_CD == null : this.RNDRG_PROV_ID_TYPE_CD.equals(that.RNDRG_PROV_ID_TYPE_CD));
    equal = equal && (this.RVNU_CD == null ? that.RVNU_CD == null : this.RVNU_CD.equals(that.RVNU_CD));
    equal = equal && (this.RVU_Flag == null ? that.RVU_Flag == null : this.RVU_Flag.equals(that.RVU_Flag));
    equal = equal && (this.SRC_APRVL_CD == null ? that.SRC_APRVL_CD == null : this.SRC_APRVL_CD.equals(that.SRC_APRVL_CD));
    equal = equal && (this.SRC_BILLG_TAX_ID == null ? that.SRC_BILLG_TAX_ID == null : this.SRC_BILLG_TAX_ID.equals(that.SRC_BILLG_TAX_ID));
    equal = equal && (this.SRC_CLM_DISP_CD == null ? that.SRC_CLM_DISP_CD == null : this.SRC_CLM_DISP_CD.equals(that.SRC_CLM_DISP_CD));
    equal = equal && (this.SRC_FINDER_NBR == null ? that.SRC_FINDER_NBR == null : this.SRC_FINDER_NBR.equals(that.SRC_FINDER_NBR));
    equal = equal && (this.SRC_GRP_NBR == null ? that.SRC_GRP_NBR == null : this.SRC_GRP_NBR.equals(that.SRC_GRP_NBR));
    equal = equal && (this.SRC_HMO_CLS_CD == null ? that.SRC_HMO_CLS_CD == null : this.SRC_HMO_CLS_CD.equals(that.SRC_HMO_CLS_CD));
    equal = equal && (this.SRC_INSTNL_NGTTD_SRVC_TERM_ID == null ? that.SRC_INSTNL_NGTTD_SRVC_TERM_ID == null : this.SRC_INSTNL_NGTTD_SRVC_TERM_ID.equals(that.SRC_INSTNL_NGTTD_SRVC_TERM_ID));
    equal = equal && (this.SRC_INSTNL_REIMBMNT_TERM_ID == null ? that.SRC_INSTNL_REIMBMNT_TERM_ID == null : this.SRC_INSTNL_REIMBMNT_TERM_ID.equals(that.SRC_INSTNL_REIMBMNT_TERM_ID));
    equal = equal && (this.SRC_MEDCR_ID == null ? that.SRC_MEDCR_ID == null : this.SRC_MEDCR_ID.equals(that.SRC_MEDCR_ID));
    equal = equal && (this.SRC_NST_SRVC_CTGRY_CD == null ? that.SRC_NST_SRVC_CTGRY_CD == null : this.SRC_NST_SRVC_CTGRY_CD.equals(that.SRC_NST_SRVC_CTGRY_CD));
    equal = equal && (this.SRC_PAY_ACTN_CD == null ? that.SRC_PAY_ACTN_CD == null : this.SRC_PAY_ACTN_CD.equals(that.SRC_PAY_ACTN_CD));
    equal = equal && (this.SRC_PRCG_CRTRIA_CD == null ? that.SRC_PRCG_CRTRIA_CD == null : this.SRC_PRCG_CRTRIA_CD.equals(that.SRC_PRCG_CRTRIA_CD));
    equal = equal && (this.SRC_PRCG_RSN_CD == null ? that.SRC_PRCG_RSN_CD == null : this.SRC_PRCG_RSN_CD.equals(that.SRC_PRCG_RSN_CD));
    equal = equal && (this.SRC_PROV_NATL_PROV_ID == null ? that.SRC_PROV_NATL_PROV_ID == null : this.SRC_PROV_NATL_PROV_ID.equals(that.SRC_PROV_NATL_PROV_ID));
    equal = equal && (this.SRC_PRTY_SCOR_NBR == null ? that.SRC_PRTY_SCOR_NBR == null : this.SRC_PRTY_SCOR_NBR.equals(that.SRC_PRTY_SCOR_NBR));
    equal = equal && (this.SRC_SRVC_CLSFCTN_CD == null ? that.SRC_SRVC_CLSFCTN_CD == null : this.SRC_SRVC_CLSFCTN_CD.equals(that.SRC_SRVC_CLSFCTN_CD));
    equal = equal && (this.SRC_TERM_EFCTV_DT == null ? that.SRC_TERM_EFCTV_DT == null : this.SRC_TERM_EFCTV_DT.equals(that.SRC_TERM_EFCTV_DT));
    equal = equal && (this.SRC_TERM_END_DT == null ? that.SRC_TERM_END_DT == null : this.SRC_TERM_END_DT.equals(that.SRC_TERM_END_DT));
    equal = equal && (this.TOS_CD == null ? that.TOS_CD == null : this.TOS_CD.equals(that.TOS_CD));
    equal = equal && (this.apcst == null ? that.apcst == null : this.apcst.equals(that.apcst));
    equal = equal && (this.apcwt == null ? that.apcwt == null : this.apcwt.equals(that.apcwt));
    equal = equal && (this.betos == null ? that.betos == null : this.betos.equals(that.betos));
    equal = equal && (this.brand == null ? that.brand == null : this.brand.equals(that.brand));
    equal = equal && (this.cat1 == null ? that.cat1 == null : this.cat1.equals(that.cat1));
    equal = equal && (this.cat2 == null ? that.cat2 == null : this.cat2.equals(that.cat2));
    equal = equal && (this.clmwt == null ? that.clmwt == null : this.clmwt.equals(that.clmwt));
    equal = equal && (this.erwt == null ? that.erwt == null : this.erwt.equals(that.erwt));
    equal = equal && (this.fundlvl2 == null ? that.fundlvl2 == null : this.fundlvl2.equals(that.fundlvl2));
    equal = equal && (this.liccd == null ? that.liccd == null : this.liccd.equals(that.liccd));
    equal = equal && (this.market == null ? that.market == null : this.market.equals(that.market));
    equal = equal && (this.Inc_Month == null ? that.Inc_Month == null : this.Inc_Month.equals(that.Inc_Month));
    equal = equal && (this.network == null ? that.network == null : this.network.equals(that.network));
    equal = equal && (this.normflag == null ? that.normflag == null : this.normflag.equals(that.normflag));
    equal = equal && (this.priority == null ? that.priority == null : this.priority.equals(that.priority));
    equal = equal && (this.prodlvl3 == null ? that.prodlvl3 == null : this.prodlvl3.equals(that.prodlvl3));
    equal = equal && (this.prov_county == null ? that.prov_county == null : this.prov_county.equals(that.prov_county));
    equal = equal && (this.surgrp == null ? that.surgrp == null : this.surgrp.equals(that.surgrp));
    equal = equal && (this.surgwt == null ? that.surgwt == null : this.surgwt.equals(that.surgwt));
    equal = equal && (this.system_id == null ? that.system_id == null : this.system_id.equals(that.system_id));
    equal = equal && (this.GRP_NM == null ? that.GRP_NM == null : this.GRP_NM.equals(that.GRP_NM));
    equal = equal && (this.SRC_SUBGRP_NBR == null ? that.SRC_SUBGRP_NBR == null : this.SRC_SUBGRP_NBR.equals(that.SRC_SUBGRP_NBR));
    equal = equal && (this.VISITS == null ? that.VISITS == null : this.VISITS.equals(that.VISITS));
    equal = equal && (this.GRP_SIC == null ? that.GRP_SIC == null : this.GRP_SIC.equals(that.GRP_SIC));
    equal = equal && (this.MCID == null ? that.MCID == null : this.MCID.equals(that.MCID));
    equal = equal && (this.CLM_ITS_SCCF_NBR == null ? that.CLM_ITS_SCCF_NBR == null : this.CLM_ITS_SCCF_NBR.equals(that.CLM_ITS_SCCF_NBR));
    equal = equal && (this.MBRSHP_SOR_CD == null ? that.MBRSHP_SOR_CD == null : this.MBRSHP_SOR_CD.equals(that.MBRSHP_SOR_CD));
    equal = equal && (this.SRC_RT_CTGRY_CD == null ? that.SRC_RT_CTGRY_CD == null : this.SRC_RT_CTGRY_CD.equals(that.SRC_RT_CTGRY_CD));
    equal = equal && (this.SRC_RT_CTGRY_SUB_CD == null ? that.SRC_RT_CTGRY_SUB_CD == null : this.SRC_RT_CTGRY_SUB_CD.equals(that.SRC_RT_CTGRY_SUB_CD));
    equal = equal && (this.run_date == null ? that.run_date == null : this.run_date.equals(that.run_date));
    equal = equal && (this.CLM_LINE_SRVC_STRT_DT == null ? that.CLM_LINE_SRVC_STRT_DT == null : this.CLM_LINE_SRVC_STRT_DT.equals(that.CLM_LINE_SRVC_STRT_DT));
    equal = equal && (this.VNDR_CD == null ? that.VNDR_CD == null : this.VNDR_CD.equals(that.VNDR_CD));
    equal = equal && (this.REPRICE_CD == null ? that.REPRICE_CD == null : this.REPRICE_CD.equals(that.REPRICE_CD));
    equal = equal && (this.VNDR_PROD_CD == null ? that.VNDR_PROD_CD == null : this.VNDR_PROD_CD.equals(that.VNDR_PROD_CD));
    equal = equal && (this.zip3 == null ? that.zip3 == null : this.zip3.equals(that.zip3));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.ADJDCTN_DT == null ? that.ADJDCTN_DT == null : this.ADJDCTN_DT.equals(that.ADJDCTN_DT));
    equal = equal && (this.ADJDCTN_STTS == null ? that.ADJDCTN_STTS == null : this.ADJDCTN_STTS.equals(that.ADJDCTN_STTS));
    equal = equal && (this.ADMSN_TYPE_CD == null ? that.ADMSN_TYPE_CD == null : this.ADMSN_TYPE_CD.equals(that.ADMSN_TYPE_CD));
    equal = equal && (this.ADMT_DT == null ? that.ADMT_DT == null : this.ADMT_DT.equals(that.ADMT_DT));
    equal = equal && (this.ALWD_AMT == null ? that.ALWD_AMT == null : this.ALWD_AMT.equals(that.ALWD_AMT));
    equal = equal && (this.APC_MEDCR_ID == null ? that.APC_MEDCR_ID == null : this.APC_MEDCR_ID.equals(that.APC_MEDCR_ID));
    equal = equal && (this.APC_VRSN_NBR == null ? that.APC_VRSN_NBR == null : this.APC_VRSN_NBR.equals(that.APC_VRSN_NBR));
    equal = equal && (this.BED_TYPE_CD == null ? that.BED_TYPE_CD == null : this.BED_TYPE_CD.equals(that.BED_TYPE_CD));
    equal = equal && (this.BILLD_CHRG_AMT == null ? that.BILLD_CHRG_AMT == null : this.BILLD_CHRG_AMT.equals(that.BILLD_CHRG_AMT));
    equal = equal && (this.BILLD_SRVC_UNIT_CNT == null ? that.BILLD_SRVC_UNIT_CNT == null : this.BILLD_SRVC_UNIT_CNT.equals(that.BILLD_SRVC_UNIT_CNT));
    equal = equal && (this.BNFT_PAYMNT_STTS_CD == null ? that.BNFT_PAYMNT_STTS_CD == null : this.BNFT_PAYMNT_STTS_CD.equals(that.BNFT_PAYMNT_STTS_CD));
    equal = equal && (this.CASES == null ? that.CASES == null : this.CASES.equals(that.CASES));
    equal = equal && (this.CLM_ADJSTMNT_KEY == null ? that.CLM_ADJSTMNT_KEY == null : this.CLM_ADJSTMNT_KEY.equals(that.CLM_ADJSTMNT_KEY));
    equal = equal && (this.CLM_ADJSTMNT_NBR == null ? that.CLM_ADJSTMNT_NBR == null : this.CLM_ADJSTMNT_NBR.equals(that.CLM_ADJSTMNT_NBR));
    equal = equal && (this.CLM_DISP_CD == null ? that.CLM_DISP_CD == null : this.CLM_DISP_CD.equals(that.CLM_DISP_CD));
    equal = equal && (this.CLM_ITS_HOST_CD == null ? that.CLM_ITS_HOST_CD == null : this.CLM_ITS_HOST_CD.equals(that.CLM_ITS_HOST_CD));
    equal = equal && (this.CLM_LINE_NBR == null ? that.CLM_LINE_NBR == null : this.CLM_LINE_NBR.equals(that.CLM_LINE_NBR));
    equal = equal && (this.CLM_LINE_REIMB_MTHD_CD == null ? that.CLM_LINE_REIMB_MTHD_CD == null : this.CLM_LINE_REIMB_MTHD_CD.equals(that.CLM_LINE_REIMB_MTHD_CD));
    equal = equal && (this.CLM_NBR == null ? that.CLM_NBR == null : this.CLM_NBR.equals(that.CLM_NBR));
    equal = equal && (this.CLM_REIMBMNT_SOR_CD == null ? that.CLM_REIMBMNT_SOR_CD == null : this.CLM_REIMBMNT_SOR_CD.equals(that.CLM_REIMBMNT_SOR_CD));
    equal = equal && (this.CLM_REIMBMNT_TYPE_CD == null ? that.CLM_REIMBMNT_TYPE_CD == null : this.CLM_REIMBMNT_TYPE_CD.equals(that.CLM_REIMBMNT_TYPE_CD));
    equal = equal && (this.CLM_SOR_CD == null ? that.CLM_SOR_CD == null : this.CLM_SOR_CD.equals(that.CLM_SOR_CD));
    equal = equal && (this.CLM_STMNT_FROM_DT == null ? that.CLM_STMNT_FROM_DT == null : this.CLM_STMNT_FROM_DT.equals(that.CLM_STMNT_FROM_DT));
    equal = equal && (this.CLM_STMNT_TO_DT == null ? that.CLM_STMNT_TO_DT == null : this.CLM_STMNT_TO_DT.equals(that.CLM_STMNT_TO_DT));
    equal = equal && (this.CMAD == null ? that.CMAD == null : this.CMAD.equals(that.CMAD));
    equal = equal && (this.CMAD1 == null ? that.CMAD1 == null : this.CMAD1.equals(that.CMAD1));
    equal = equal && (this.CMAD_ALLOWED == null ? that.CMAD_ALLOWED == null : this.CMAD_ALLOWED.equals(that.CMAD_ALLOWED));
    equal = equal && (this.CMAD_BILLED == null ? that.CMAD_BILLED == null : this.CMAD_BILLED.equals(that.CMAD_BILLED));
    equal = equal && (this.CMAD_CASES == null ? that.CMAD_CASES == null : this.CMAD_CASES.equals(that.CMAD_CASES));
    equal = equal && (this.CMPNY_CF_CD == null ? that.CMPNY_CF_CD == null : this.CMPNY_CF_CD.equals(that.CMPNY_CF_CD));
    equal = equal && (this.CVRD_EXPNS_AMT == null ? that.CVRD_EXPNS_AMT == null : this.CVRD_EXPNS_AMT.equals(that.CVRD_EXPNS_AMT));
    equal = equal && (this.DERIVD_IND_CD == null ? that.DERIVD_IND_CD == null : this.DERIVD_IND_CD.equals(that.DERIVD_IND_CD));
    equal = equal && (this.DSCHRG_DT == null ? that.DSCHRG_DT == null : this.DSCHRG_DT.equals(that.DSCHRG_DT));
    equal = equal && (this.DSCHRG_STTS_CD == null ? that.DSCHRG_STTS_CD == null : this.DSCHRG_STTS_CD.equals(that.DSCHRG_STTS_CD));
    equal = equal && (this.ER_Flag == null ? that.ER_Flag == null : this.ER_Flag.equals(that.ER_Flag));
    equal = equal && (this.PediatricFlag == null ? that.PediatricFlag == null : this.PediatricFlag.equals(that.PediatricFlag));
    equal = equal && (this.ENC_Flag == null ? that.ENC_Flag == null : this.ENC_Flag.equals(that.ENC_Flag));
    equal = equal && (this.EXCHNG_CERTFN_CD == null ? that.EXCHNG_CERTFN_CD == null : this.EXCHNG_CERTFN_CD.equals(that.EXCHNG_CERTFN_CD));
    equal = equal && (this.EXCHNG_IND_CD == null ? that.EXCHNG_IND_CD == null : this.EXCHNG_IND_CD.equals(that.EXCHNG_IND_CD));
    equal = equal && (this.EXCHNG_METAL_TYPE_CD == null ? that.EXCHNG_METAL_TYPE_CD == null : this.EXCHNG_METAL_TYPE_CD.equals(that.EXCHNG_METAL_TYPE_CD));
    equal = equal && (this.FUNDG_CF_CD == null ? that.FUNDG_CF_CD == null : this.FUNDG_CF_CD.equals(that.FUNDG_CF_CD));
    equal = equal && (this.HLTH_SRVC_CD == null ? that.HLTH_SRVC_CD == null : this.HLTH_SRVC_CD.equals(that.HLTH_SRVC_CD));
    equal = equal && (this.PROC_MDFR_1_CD == null ? that.PROC_MDFR_1_CD == null : this.PROC_MDFR_1_CD.equals(that.PROC_MDFR_1_CD));
    equal = equal && (this.PROC_MDFR_2_CD == null ? that.PROC_MDFR_2_CD == null : this.PROC_MDFR_2_CD.equals(that.PROC_MDFR_2_CD));
    equal = equal && (this.HMO_CLS_CD == null ? that.HMO_CLS_CD == null : this.HMO_CLS_CD.equals(that.HMO_CLS_CD));
    equal = equal && (this.ICL_ALWD_AMT == null ? that.ICL_ALWD_AMT == null : this.ICL_ALWD_AMT.equals(that.ICL_ALWD_AMT));
    equal = equal && (this.ICL_PAID_AMT == null ? that.ICL_PAID_AMT == null : this.ICL_PAID_AMT.equals(that.ICL_PAID_AMT));
    equal = equal && (this.INN_CD == null ? that.INN_CD == null : this.INN_CD.equals(that.INN_CD));
    equal = equal && (this.LEGACY == null ? that.LEGACY == null : this.LEGACY.equals(that.LEGACY));
    equal = equal && (this.LINE_ITEM_APC_WT_AMT == null ? that.LINE_ITEM_APC_WT_AMT == null : this.LINE_ITEM_APC_WT_AMT.equals(that.LINE_ITEM_APC_WT_AMT));
    equal = equal && (this.LINE_ITEM_PAYMNT_AMT == null ? that.LINE_ITEM_PAYMNT_AMT == null : this.LINE_ITEM_PAYMNT_AMT.equals(that.LINE_ITEM_PAYMNT_AMT));
    equal = equal && (this.MBR_County == null ? that.MBR_County == null : this.MBR_County.equals(that.MBR_County));
    equal = equal && (this.MBR_KEY == null ? that.MBR_KEY == null : this.MBR_KEY.equals(that.MBR_KEY));
    equal = equal && (this.MBR_State == null ? that.MBR_State == null : this.MBR_State.equals(that.MBR_State));
    equal = equal && (this.MBR_ZIP3 == null ? that.MBR_ZIP3 == null : this.MBR_ZIP3.equals(that.MBR_ZIP3));
    equal = equal && (this.MBR_ZIP_CD == null ? that.MBR_ZIP_CD == null : this.MBR_ZIP_CD.equals(that.MBR_ZIP_CD));
    equal = equal && (this.MBU_CF_CD == null ? that.MBU_CF_CD == null : this.MBU_CF_CD.equals(that.MBU_CF_CD));
    equal = equal && (this.MBUlvl1 == null ? that.MBUlvl1 == null : this.MBUlvl1.equals(that.MBUlvl1));
    equal = equal && (this.MBUlvl2 == null ? that.MBUlvl2 == null : this.MBUlvl2.equals(that.MBUlvl2));
    equal = equal && (this.MBUlvl3 == null ? that.MBUlvl3 == null : this.MBUlvl3.equals(that.MBUlvl3));
    equal = equal && (this.MBUlvl4 == null ? that.MBUlvl4 == null : this.MBUlvl4.equals(that.MBUlvl4));
    equal = equal && (this.MCS == null ? that.MCS == null : this.MCS.equals(that.MCS));
    equal = equal && (this.MEDCR_ID == null ? that.MEDCR_ID == null : this.MEDCR_ID.equals(that.MEDCR_ID));
    equal = equal && (this.NRMLZD_AMT == null ? that.NRMLZD_AMT == null : this.NRMLZD_AMT.equals(that.NRMLZD_AMT));
    equal = equal && (this.NRMLZD_CASE == null ? that.NRMLZD_CASE == null : this.NRMLZD_CASE.equals(that.NRMLZD_CASE));
    equal = equal && (this.NTWK_ID == null ? that.NTWK_ID == null : this.NTWK_ID.equals(that.NTWK_ID));
    equal = equal && (this.CLM_NTWK_KEY == null ? that.CLM_NTWK_KEY == null : this.CLM_NTWK_KEY.equals(that.CLM_NTWK_KEY));
    equal = equal && (this.OBS_Flag == null ? that.OBS_Flag == null : this.OBS_Flag.equals(that.OBS_Flag));
    equal = equal && (this.OUT_ITEM_STTS_IND_TXT == null ? that.OUT_ITEM_STTS_IND_TXT == null : this.OUT_ITEM_STTS_IND_TXT.equals(that.OUT_ITEM_STTS_IND_TXT));
    equal = equal && (this.PAID_AMT == null ? that.PAID_AMT == null : this.PAID_AMT.equals(that.PAID_AMT));
    equal = equal && (this.PAID_SRVC_UNIT_CNT == null ? that.PAID_SRVC_UNIT_CNT == null : this.PAID_SRVC_UNIT_CNT.equals(that.PAID_SRVC_UNIT_CNT));
    equal = equal && (this.PARG_STTS_CD == null ? that.PARG_STTS_CD == null : this.PARG_STTS_CD.equals(that.PARG_STTS_CD));
    equal = equal && (this.PAYMNT_APC_NBR == null ? that.PAYMNT_APC_NBR == null : this.PAYMNT_APC_NBR.equals(that.PAYMNT_APC_NBR));
    equal = equal && (this.PN_ID == null ? that.PN_ID == null : this.PN_ID.equals(that.PN_ID));
    equal = equal && (this.PROD_CF_CD == null ? that.PROD_CF_CD == null : this.PROD_CF_CD.equals(that.PROD_CF_CD));
    equal = equal && (this.PROD_ID == null ? that.PROD_ID == null : this.PROD_ID.equals(that.PROD_ID));
    equal = equal && (this.PROV_CITY_NM == null ? that.PROV_CITY_NM == null : this.PROV_CITY_NM.equals(that.PROV_CITY_NM));
    equal = equal && (this.PROV_ID == null ? that.PROV_ID == null : this.PROV_ID.equals(that.PROV_ID));
    equal = equal && (this.PROV_NM == null ? that.PROV_NM == null : this.PROV_NM.equals(that.PROV_NM));
    equal = equal && (this.PROV_ST_NM == null ? that.PROV_ST_NM == null : this.PROV_ST_NM.equals(that.PROV_ST_NM));
    equal = equal && (this.PROV_ZIP_CD == null ? that.PROV_ZIP_CD == null : this.PROV_ZIP_CD.equals(that.PROV_ZIP_CD));
    equal = equal && (this.RDCTN_CTGRY_CD == null ? that.RDCTN_CTGRY_CD == null : this.RDCTN_CTGRY_CD.equals(that.RDCTN_CTGRY_CD));
    equal = equal && (this.RELATIVE_WEIGHT == null ? that.RELATIVE_WEIGHT == null : this.RELATIVE_WEIGHT.equals(that.RELATIVE_WEIGHT));
    equal = equal && (this.RELATIVE_WEIGHT1 == null ? that.RELATIVE_WEIGHT1 == null : this.RELATIVE_WEIGHT1.equals(that.RELATIVE_WEIGHT1));
    equal = equal && (this.RNDRG_PROV_ID == null ? that.RNDRG_PROV_ID == null : this.RNDRG_PROV_ID.equals(that.RNDRG_PROV_ID));
    equal = equal && (this.RNDRG_PROV_ID_TYPE_CD == null ? that.RNDRG_PROV_ID_TYPE_CD == null : this.RNDRG_PROV_ID_TYPE_CD.equals(that.RNDRG_PROV_ID_TYPE_CD));
    equal = equal && (this.RVNU_CD == null ? that.RVNU_CD == null : this.RVNU_CD.equals(that.RVNU_CD));
    equal = equal && (this.RVU_Flag == null ? that.RVU_Flag == null : this.RVU_Flag.equals(that.RVU_Flag));
    equal = equal && (this.SRC_APRVL_CD == null ? that.SRC_APRVL_CD == null : this.SRC_APRVL_CD.equals(that.SRC_APRVL_CD));
    equal = equal && (this.SRC_BILLG_TAX_ID == null ? that.SRC_BILLG_TAX_ID == null : this.SRC_BILLG_TAX_ID.equals(that.SRC_BILLG_TAX_ID));
    equal = equal && (this.SRC_CLM_DISP_CD == null ? that.SRC_CLM_DISP_CD == null : this.SRC_CLM_DISP_CD.equals(that.SRC_CLM_DISP_CD));
    equal = equal && (this.SRC_FINDER_NBR == null ? that.SRC_FINDER_NBR == null : this.SRC_FINDER_NBR.equals(that.SRC_FINDER_NBR));
    equal = equal && (this.SRC_GRP_NBR == null ? that.SRC_GRP_NBR == null : this.SRC_GRP_NBR.equals(that.SRC_GRP_NBR));
    equal = equal && (this.SRC_HMO_CLS_CD == null ? that.SRC_HMO_CLS_CD == null : this.SRC_HMO_CLS_CD.equals(that.SRC_HMO_CLS_CD));
    equal = equal && (this.SRC_INSTNL_NGTTD_SRVC_TERM_ID == null ? that.SRC_INSTNL_NGTTD_SRVC_TERM_ID == null : this.SRC_INSTNL_NGTTD_SRVC_TERM_ID.equals(that.SRC_INSTNL_NGTTD_SRVC_TERM_ID));
    equal = equal && (this.SRC_INSTNL_REIMBMNT_TERM_ID == null ? that.SRC_INSTNL_REIMBMNT_TERM_ID == null : this.SRC_INSTNL_REIMBMNT_TERM_ID.equals(that.SRC_INSTNL_REIMBMNT_TERM_ID));
    equal = equal && (this.SRC_MEDCR_ID == null ? that.SRC_MEDCR_ID == null : this.SRC_MEDCR_ID.equals(that.SRC_MEDCR_ID));
    equal = equal && (this.SRC_NST_SRVC_CTGRY_CD == null ? that.SRC_NST_SRVC_CTGRY_CD == null : this.SRC_NST_SRVC_CTGRY_CD.equals(that.SRC_NST_SRVC_CTGRY_CD));
    equal = equal && (this.SRC_PAY_ACTN_CD == null ? that.SRC_PAY_ACTN_CD == null : this.SRC_PAY_ACTN_CD.equals(that.SRC_PAY_ACTN_CD));
    equal = equal && (this.SRC_PRCG_CRTRIA_CD == null ? that.SRC_PRCG_CRTRIA_CD == null : this.SRC_PRCG_CRTRIA_CD.equals(that.SRC_PRCG_CRTRIA_CD));
    equal = equal && (this.SRC_PRCG_RSN_CD == null ? that.SRC_PRCG_RSN_CD == null : this.SRC_PRCG_RSN_CD.equals(that.SRC_PRCG_RSN_CD));
    equal = equal && (this.SRC_PROV_NATL_PROV_ID == null ? that.SRC_PROV_NATL_PROV_ID == null : this.SRC_PROV_NATL_PROV_ID.equals(that.SRC_PROV_NATL_PROV_ID));
    equal = equal && (this.SRC_PRTY_SCOR_NBR == null ? that.SRC_PRTY_SCOR_NBR == null : this.SRC_PRTY_SCOR_NBR.equals(that.SRC_PRTY_SCOR_NBR));
    equal = equal && (this.SRC_SRVC_CLSFCTN_CD == null ? that.SRC_SRVC_CLSFCTN_CD == null : this.SRC_SRVC_CLSFCTN_CD.equals(that.SRC_SRVC_CLSFCTN_CD));
    equal = equal && (this.SRC_TERM_EFCTV_DT == null ? that.SRC_TERM_EFCTV_DT == null : this.SRC_TERM_EFCTV_DT.equals(that.SRC_TERM_EFCTV_DT));
    equal = equal && (this.SRC_TERM_END_DT == null ? that.SRC_TERM_END_DT == null : this.SRC_TERM_END_DT.equals(that.SRC_TERM_END_DT));
    equal = equal && (this.TOS_CD == null ? that.TOS_CD == null : this.TOS_CD.equals(that.TOS_CD));
    equal = equal && (this.apcst == null ? that.apcst == null : this.apcst.equals(that.apcst));
    equal = equal && (this.apcwt == null ? that.apcwt == null : this.apcwt.equals(that.apcwt));
    equal = equal && (this.betos == null ? that.betos == null : this.betos.equals(that.betos));
    equal = equal && (this.brand == null ? that.brand == null : this.brand.equals(that.brand));
    equal = equal && (this.cat1 == null ? that.cat1 == null : this.cat1.equals(that.cat1));
    equal = equal && (this.cat2 == null ? that.cat2 == null : this.cat2.equals(that.cat2));
    equal = equal && (this.clmwt == null ? that.clmwt == null : this.clmwt.equals(that.clmwt));
    equal = equal && (this.erwt == null ? that.erwt == null : this.erwt.equals(that.erwt));
    equal = equal && (this.fundlvl2 == null ? that.fundlvl2 == null : this.fundlvl2.equals(that.fundlvl2));
    equal = equal && (this.liccd == null ? that.liccd == null : this.liccd.equals(that.liccd));
    equal = equal && (this.market == null ? that.market == null : this.market.equals(that.market));
    equal = equal && (this.Inc_Month == null ? that.Inc_Month == null : this.Inc_Month.equals(that.Inc_Month));
    equal = equal && (this.network == null ? that.network == null : this.network.equals(that.network));
    equal = equal && (this.normflag == null ? that.normflag == null : this.normflag.equals(that.normflag));
    equal = equal && (this.priority == null ? that.priority == null : this.priority.equals(that.priority));
    equal = equal && (this.prodlvl3 == null ? that.prodlvl3 == null : this.prodlvl3.equals(that.prodlvl3));
    equal = equal && (this.prov_county == null ? that.prov_county == null : this.prov_county.equals(that.prov_county));
    equal = equal && (this.surgrp == null ? that.surgrp == null : this.surgrp.equals(that.surgrp));
    equal = equal && (this.surgwt == null ? that.surgwt == null : this.surgwt.equals(that.surgwt));
    equal = equal && (this.system_id == null ? that.system_id == null : this.system_id.equals(that.system_id));
    equal = equal && (this.GRP_NM == null ? that.GRP_NM == null : this.GRP_NM.equals(that.GRP_NM));
    equal = equal && (this.SRC_SUBGRP_NBR == null ? that.SRC_SUBGRP_NBR == null : this.SRC_SUBGRP_NBR.equals(that.SRC_SUBGRP_NBR));
    equal = equal && (this.VISITS == null ? that.VISITS == null : this.VISITS.equals(that.VISITS));
    equal = equal && (this.GRP_SIC == null ? that.GRP_SIC == null : this.GRP_SIC.equals(that.GRP_SIC));
    equal = equal && (this.MCID == null ? that.MCID == null : this.MCID.equals(that.MCID));
    equal = equal && (this.CLM_ITS_SCCF_NBR == null ? that.CLM_ITS_SCCF_NBR == null : this.CLM_ITS_SCCF_NBR.equals(that.CLM_ITS_SCCF_NBR));
    equal = equal && (this.MBRSHP_SOR_CD == null ? that.MBRSHP_SOR_CD == null : this.MBRSHP_SOR_CD.equals(that.MBRSHP_SOR_CD));
    equal = equal && (this.SRC_RT_CTGRY_CD == null ? that.SRC_RT_CTGRY_CD == null : this.SRC_RT_CTGRY_CD.equals(that.SRC_RT_CTGRY_CD));
    equal = equal && (this.SRC_RT_CTGRY_SUB_CD == null ? that.SRC_RT_CTGRY_SUB_CD == null : this.SRC_RT_CTGRY_SUB_CD.equals(that.SRC_RT_CTGRY_SUB_CD));
    equal = equal && (this.run_date == null ? that.run_date == null : this.run_date.equals(that.run_date));
    equal = equal && (this.CLM_LINE_SRVC_STRT_DT == null ? that.CLM_LINE_SRVC_STRT_DT == null : this.CLM_LINE_SRVC_STRT_DT.equals(that.CLM_LINE_SRVC_STRT_DT));
    equal = equal && (this.VNDR_CD == null ? that.VNDR_CD == null : this.VNDR_CD.equals(that.VNDR_CD));
    equal = equal && (this.REPRICE_CD == null ? that.REPRICE_CD == null : this.REPRICE_CD.equals(that.REPRICE_CD));
    equal = equal && (this.VNDR_PROD_CD == null ? that.VNDR_PROD_CD == null : this.VNDR_PROD_CD.equals(that.VNDR_PROD_CD));
    equal = equal && (this.zip3 == null ? that.zip3 == null : this.zip3.equals(that.zip3));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.ADJDCTN_DT = JdbcWritableBridge.readDate(1, __dbResults);
    this.ADJDCTN_STTS = JdbcWritableBridge.readString(2, __dbResults);
    this.ADMSN_TYPE_CD = JdbcWritableBridge.readString(3, __dbResults);
    this.ADMT_DT = JdbcWritableBridge.readDate(4, __dbResults);
    this.ALWD_AMT = JdbcWritableBridge.readDouble(5, __dbResults);
    this.APC_MEDCR_ID = JdbcWritableBridge.readString(6, __dbResults);
    this.APC_VRSN_NBR = JdbcWritableBridge.readString(7, __dbResults);
    this.BED_TYPE_CD = JdbcWritableBridge.readString(8, __dbResults);
    this.BILLD_CHRG_AMT = JdbcWritableBridge.readDouble(9, __dbResults);
    this.BILLD_SRVC_UNIT_CNT = JdbcWritableBridge.readDouble(10, __dbResults);
    this.BNFT_PAYMNT_STTS_CD = JdbcWritableBridge.readString(11, __dbResults);
    this.CASES = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.CLM_ADJSTMNT_KEY = JdbcWritableBridge.readString(13, __dbResults);
    this.CLM_ADJSTMNT_NBR = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.CLM_DISP_CD = JdbcWritableBridge.readString(15, __dbResults);
    this.CLM_ITS_HOST_CD = JdbcWritableBridge.readString(16, __dbResults);
    this.CLM_LINE_NBR = JdbcWritableBridge.readString(17, __dbResults);
    this.CLM_LINE_REIMB_MTHD_CD = JdbcWritableBridge.readString(18, __dbResults);
    this.CLM_NBR = JdbcWritableBridge.readString(19, __dbResults);
    this.CLM_REIMBMNT_SOR_CD = JdbcWritableBridge.readString(20, __dbResults);
    this.CLM_REIMBMNT_TYPE_CD = JdbcWritableBridge.readString(21, __dbResults);
    this.CLM_SOR_CD = JdbcWritableBridge.readString(22, __dbResults);
    this.CLM_STMNT_FROM_DT = JdbcWritableBridge.readDate(23, __dbResults);
    this.CLM_STMNT_TO_DT = JdbcWritableBridge.readDate(24, __dbResults);
    this.CMAD = JdbcWritableBridge.readDouble(25, __dbResults);
    this.CMAD1 = JdbcWritableBridge.readDouble(26, __dbResults);
    this.CMAD_ALLOWED = JdbcWritableBridge.readDouble(27, __dbResults);
    this.CMAD_BILLED = JdbcWritableBridge.readDouble(28, __dbResults);
    this.CMAD_CASES = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.CMPNY_CF_CD = JdbcWritableBridge.readString(30, __dbResults);
    this.CVRD_EXPNS_AMT = JdbcWritableBridge.readDouble(31, __dbResults);
    this.DERIVD_IND_CD = JdbcWritableBridge.readString(32, __dbResults);
    this.DSCHRG_DT = JdbcWritableBridge.readDate(33, __dbResults);
    this.DSCHRG_STTS_CD = JdbcWritableBridge.readString(34, __dbResults);
    this.ER_Flag = JdbcWritableBridge.readString(35, __dbResults);
    this.PediatricFlag = JdbcWritableBridge.readString(36, __dbResults);
    this.ENC_Flag = JdbcWritableBridge.readString(37, __dbResults);
    this.EXCHNG_CERTFN_CD = JdbcWritableBridge.readString(38, __dbResults);
    this.EXCHNG_IND_CD = JdbcWritableBridge.readString(39, __dbResults);
    this.EXCHNG_METAL_TYPE_CD = JdbcWritableBridge.readString(40, __dbResults);
    this.FUNDG_CF_CD = JdbcWritableBridge.readString(41, __dbResults);
    this.HLTH_SRVC_CD = JdbcWritableBridge.readString(42, __dbResults);
    this.PROC_MDFR_1_CD = JdbcWritableBridge.readString(43, __dbResults);
    this.PROC_MDFR_2_CD = JdbcWritableBridge.readString(44, __dbResults);
    this.HMO_CLS_CD = JdbcWritableBridge.readString(45, __dbResults);
    this.ICL_ALWD_AMT = JdbcWritableBridge.readDouble(46, __dbResults);
    this.ICL_PAID_AMT = JdbcWritableBridge.readDouble(47, __dbResults);
    this.INN_CD = JdbcWritableBridge.readString(48, __dbResults);
    this.LEGACY = JdbcWritableBridge.readString(49, __dbResults);
    this.LINE_ITEM_APC_WT_AMT = JdbcWritableBridge.readDouble(50, __dbResults);
    this.LINE_ITEM_PAYMNT_AMT = JdbcWritableBridge.readDouble(51, __dbResults);
    this.MBR_County = JdbcWritableBridge.readString(52, __dbResults);
    this.MBR_KEY = JdbcWritableBridge.readString(53, __dbResults);
    this.MBR_State = JdbcWritableBridge.readString(54, __dbResults);
    this.MBR_ZIP3 = JdbcWritableBridge.readString(55, __dbResults);
    this.MBR_ZIP_CD = JdbcWritableBridge.readString(56, __dbResults);
    this.MBU_CF_CD = JdbcWritableBridge.readString(57, __dbResults);
    this.MBUlvl1 = JdbcWritableBridge.readString(58, __dbResults);
    this.MBUlvl2 = JdbcWritableBridge.readString(59, __dbResults);
    this.MBUlvl3 = JdbcWritableBridge.readString(60, __dbResults);
    this.MBUlvl4 = JdbcWritableBridge.readString(61, __dbResults);
    this.MCS = JdbcWritableBridge.readString(62, __dbResults);
    this.MEDCR_ID = JdbcWritableBridge.readString(63, __dbResults);
    this.NRMLZD_AMT = JdbcWritableBridge.readDouble(64, __dbResults);
    this.NRMLZD_CASE = JdbcWritableBridge.readBigDecimal(65, __dbResults);
    this.NTWK_ID = JdbcWritableBridge.readString(66, __dbResults);
    this.CLM_NTWK_KEY = JdbcWritableBridge.readString(67, __dbResults);
    this.OBS_Flag = JdbcWritableBridge.readString(68, __dbResults);
    this.OUT_ITEM_STTS_IND_TXT = JdbcWritableBridge.readString(69, __dbResults);
    this.PAID_AMT = JdbcWritableBridge.readDouble(70, __dbResults);
    this.PAID_SRVC_UNIT_CNT = JdbcWritableBridge.readBigDecimal(71, __dbResults);
    this.PARG_STTS_CD = JdbcWritableBridge.readString(72, __dbResults);
    this.PAYMNT_APC_NBR = JdbcWritableBridge.readString(73, __dbResults);
    this.PN_ID = JdbcWritableBridge.readString(74, __dbResults);
    this.PROD_CF_CD = JdbcWritableBridge.readString(75, __dbResults);
    this.PROD_ID = JdbcWritableBridge.readString(76, __dbResults);
    this.PROV_CITY_NM = JdbcWritableBridge.readString(77, __dbResults);
    this.PROV_ID = JdbcWritableBridge.readString(78, __dbResults);
    this.PROV_NM = JdbcWritableBridge.readString(79, __dbResults);
    this.PROV_ST_NM = JdbcWritableBridge.readString(80, __dbResults);
    this.PROV_ZIP_CD = JdbcWritableBridge.readString(81, __dbResults);
    this.RDCTN_CTGRY_CD = JdbcWritableBridge.readString(82, __dbResults);
    this.RELATIVE_WEIGHT = JdbcWritableBridge.readDouble(83, __dbResults);
    this.RELATIVE_WEIGHT1 = JdbcWritableBridge.readDouble(84, __dbResults);
    this.RNDRG_PROV_ID = JdbcWritableBridge.readString(85, __dbResults);
    this.RNDRG_PROV_ID_TYPE_CD = JdbcWritableBridge.readString(86, __dbResults);
    this.RVNU_CD = JdbcWritableBridge.readString(87, __dbResults);
    this.RVU_Flag = JdbcWritableBridge.readString(88, __dbResults);
    this.SRC_APRVL_CD = JdbcWritableBridge.readString(89, __dbResults);
    this.SRC_BILLG_TAX_ID = JdbcWritableBridge.readString(90, __dbResults);
    this.SRC_CLM_DISP_CD = JdbcWritableBridge.readString(91, __dbResults);
    this.SRC_FINDER_NBR = JdbcWritableBridge.readBigDecimal(92, __dbResults);
    this.SRC_GRP_NBR = JdbcWritableBridge.readString(93, __dbResults);
    this.SRC_HMO_CLS_CD = JdbcWritableBridge.readString(94, __dbResults);
    this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = JdbcWritableBridge.readString(95, __dbResults);
    this.SRC_INSTNL_REIMBMNT_TERM_ID = JdbcWritableBridge.readString(96, __dbResults);
    this.SRC_MEDCR_ID = JdbcWritableBridge.readString(97, __dbResults);
    this.SRC_NST_SRVC_CTGRY_CD = JdbcWritableBridge.readString(98, __dbResults);
    this.SRC_PAY_ACTN_CD = JdbcWritableBridge.readString(99, __dbResults);
    this.SRC_PRCG_CRTRIA_CD = JdbcWritableBridge.readString(100, __dbResults);
    this.SRC_PRCG_RSN_CD = JdbcWritableBridge.readString(101, __dbResults);
    this.SRC_PROV_NATL_PROV_ID = JdbcWritableBridge.readString(102, __dbResults);
    this.SRC_PRTY_SCOR_NBR = JdbcWritableBridge.readBigDecimal(103, __dbResults);
    this.SRC_SRVC_CLSFCTN_CD = JdbcWritableBridge.readString(104, __dbResults);
    this.SRC_TERM_EFCTV_DT = JdbcWritableBridge.readDate(105, __dbResults);
    this.SRC_TERM_END_DT = JdbcWritableBridge.readDate(106, __dbResults);
    this.TOS_CD = JdbcWritableBridge.readString(107, __dbResults);
    this.apcst = JdbcWritableBridge.readString(108, __dbResults);
    this.apcwt = JdbcWritableBridge.readDouble(109, __dbResults);
    this.betos = JdbcWritableBridge.readString(110, __dbResults);
    this.brand = JdbcWritableBridge.readString(111, __dbResults);
    this.cat1 = JdbcWritableBridge.readString(112, __dbResults);
    this.cat2 = JdbcWritableBridge.readString(113, __dbResults);
    this.clmwt = JdbcWritableBridge.readDouble(114, __dbResults);
    this.erwt = JdbcWritableBridge.readDouble(115, __dbResults);
    this.fundlvl2 = JdbcWritableBridge.readString(116, __dbResults);
    this.liccd = JdbcWritableBridge.readString(117, __dbResults);
    this.market = JdbcWritableBridge.readString(118, __dbResults);
    this.Inc_Month = JdbcWritableBridge.readString(119, __dbResults);
    this.network = JdbcWritableBridge.readString(120, __dbResults);
    this.normflag = JdbcWritableBridge.readString(121, __dbResults);
    this.priority = JdbcWritableBridge.readString(122, __dbResults);
    this.prodlvl3 = JdbcWritableBridge.readString(123, __dbResults);
    this.prov_county = JdbcWritableBridge.readString(124, __dbResults);
    this.surgrp = JdbcWritableBridge.readString(125, __dbResults);
    this.surgwt = JdbcWritableBridge.readDouble(126, __dbResults);
    this.system_id = JdbcWritableBridge.readString(127, __dbResults);
    this.GRP_NM = JdbcWritableBridge.readString(128, __dbResults);
    this.SRC_SUBGRP_NBR = JdbcWritableBridge.readString(129, __dbResults);
    this.VISITS = JdbcWritableBridge.readDouble(130, __dbResults);
    this.GRP_SIC = JdbcWritableBridge.readString(131, __dbResults);
    this.MCID = JdbcWritableBridge.readString(132, __dbResults);
    this.CLM_ITS_SCCF_NBR = JdbcWritableBridge.readString(133, __dbResults);
    this.MBRSHP_SOR_CD = JdbcWritableBridge.readString(134, __dbResults);
    this.SRC_RT_CTGRY_CD = JdbcWritableBridge.readString(135, __dbResults);
    this.SRC_RT_CTGRY_SUB_CD = JdbcWritableBridge.readString(136, __dbResults);
    this.run_date = JdbcWritableBridge.readDate(137, __dbResults);
    this.CLM_LINE_SRVC_STRT_DT = JdbcWritableBridge.readDate(138, __dbResults);
    this.VNDR_CD = JdbcWritableBridge.readString(139, __dbResults);
    this.REPRICE_CD = JdbcWritableBridge.readString(140, __dbResults);
    this.VNDR_PROD_CD = JdbcWritableBridge.readString(141, __dbResults);
    this.zip3 = JdbcWritableBridge.readString(142, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.ADJDCTN_DT = JdbcWritableBridge.readDate(1, __dbResults);
    this.ADJDCTN_STTS = JdbcWritableBridge.readString(2, __dbResults);
    this.ADMSN_TYPE_CD = JdbcWritableBridge.readString(3, __dbResults);
    this.ADMT_DT = JdbcWritableBridge.readDate(4, __dbResults);
    this.ALWD_AMT = JdbcWritableBridge.readDouble(5, __dbResults);
    this.APC_MEDCR_ID = JdbcWritableBridge.readString(6, __dbResults);
    this.APC_VRSN_NBR = JdbcWritableBridge.readString(7, __dbResults);
    this.BED_TYPE_CD = JdbcWritableBridge.readString(8, __dbResults);
    this.BILLD_CHRG_AMT = JdbcWritableBridge.readDouble(9, __dbResults);
    this.BILLD_SRVC_UNIT_CNT = JdbcWritableBridge.readDouble(10, __dbResults);
    this.BNFT_PAYMNT_STTS_CD = JdbcWritableBridge.readString(11, __dbResults);
    this.CASES = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.CLM_ADJSTMNT_KEY = JdbcWritableBridge.readString(13, __dbResults);
    this.CLM_ADJSTMNT_NBR = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.CLM_DISP_CD = JdbcWritableBridge.readString(15, __dbResults);
    this.CLM_ITS_HOST_CD = JdbcWritableBridge.readString(16, __dbResults);
    this.CLM_LINE_NBR = JdbcWritableBridge.readString(17, __dbResults);
    this.CLM_LINE_REIMB_MTHD_CD = JdbcWritableBridge.readString(18, __dbResults);
    this.CLM_NBR = JdbcWritableBridge.readString(19, __dbResults);
    this.CLM_REIMBMNT_SOR_CD = JdbcWritableBridge.readString(20, __dbResults);
    this.CLM_REIMBMNT_TYPE_CD = JdbcWritableBridge.readString(21, __dbResults);
    this.CLM_SOR_CD = JdbcWritableBridge.readString(22, __dbResults);
    this.CLM_STMNT_FROM_DT = JdbcWritableBridge.readDate(23, __dbResults);
    this.CLM_STMNT_TO_DT = JdbcWritableBridge.readDate(24, __dbResults);
    this.CMAD = JdbcWritableBridge.readDouble(25, __dbResults);
    this.CMAD1 = JdbcWritableBridge.readDouble(26, __dbResults);
    this.CMAD_ALLOWED = JdbcWritableBridge.readDouble(27, __dbResults);
    this.CMAD_BILLED = JdbcWritableBridge.readDouble(28, __dbResults);
    this.CMAD_CASES = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.CMPNY_CF_CD = JdbcWritableBridge.readString(30, __dbResults);
    this.CVRD_EXPNS_AMT = JdbcWritableBridge.readDouble(31, __dbResults);
    this.DERIVD_IND_CD = JdbcWritableBridge.readString(32, __dbResults);
    this.DSCHRG_DT = JdbcWritableBridge.readDate(33, __dbResults);
    this.DSCHRG_STTS_CD = JdbcWritableBridge.readString(34, __dbResults);
    this.ER_Flag = JdbcWritableBridge.readString(35, __dbResults);
    this.PediatricFlag = JdbcWritableBridge.readString(36, __dbResults);
    this.ENC_Flag = JdbcWritableBridge.readString(37, __dbResults);
    this.EXCHNG_CERTFN_CD = JdbcWritableBridge.readString(38, __dbResults);
    this.EXCHNG_IND_CD = JdbcWritableBridge.readString(39, __dbResults);
    this.EXCHNG_METAL_TYPE_CD = JdbcWritableBridge.readString(40, __dbResults);
    this.FUNDG_CF_CD = JdbcWritableBridge.readString(41, __dbResults);
    this.HLTH_SRVC_CD = JdbcWritableBridge.readString(42, __dbResults);
    this.PROC_MDFR_1_CD = JdbcWritableBridge.readString(43, __dbResults);
    this.PROC_MDFR_2_CD = JdbcWritableBridge.readString(44, __dbResults);
    this.HMO_CLS_CD = JdbcWritableBridge.readString(45, __dbResults);
    this.ICL_ALWD_AMT = JdbcWritableBridge.readDouble(46, __dbResults);
    this.ICL_PAID_AMT = JdbcWritableBridge.readDouble(47, __dbResults);
    this.INN_CD = JdbcWritableBridge.readString(48, __dbResults);
    this.LEGACY = JdbcWritableBridge.readString(49, __dbResults);
    this.LINE_ITEM_APC_WT_AMT = JdbcWritableBridge.readDouble(50, __dbResults);
    this.LINE_ITEM_PAYMNT_AMT = JdbcWritableBridge.readDouble(51, __dbResults);
    this.MBR_County = JdbcWritableBridge.readString(52, __dbResults);
    this.MBR_KEY = JdbcWritableBridge.readString(53, __dbResults);
    this.MBR_State = JdbcWritableBridge.readString(54, __dbResults);
    this.MBR_ZIP3 = JdbcWritableBridge.readString(55, __dbResults);
    this.MBR_ZIP_CD = JdbcWritableBridge.readString(56, __dbResults);
    this.MBU_CF_CD = JdbcWritableBridge.readString(57, __dbResults);
    this.MBUlvl1 = JdbcWritableBridge.readString(58, __dbResults);
    this.MBUlvl2 = JdbcWritableBridge.readString(59, __dbResults);
    this.MBUlvl3 = JdbcWritableBridge.readString(60, __dbResults);
    this.MBUlvl4 = JdbcWritableBridge.readString(61, __dbResults);
    this.MCS = JdbcWritableBridge.readString(62, __dbResults);
    this.MEDCR_ID = JdbcWritableBridge.readString(63, __dbResults);
    this.NRMLZD_AMT = JdbcWritableBridge.readDouble(64, __dbResults);
    this.NRMLZD_CASE = JdbcWritableBridge.readBigDecimal(65, __dbResults);
    this.NTWK_ID = JdbcWritableBridge.readString(66, __dbResults);
    this.CLM_NTWK_KEY = JdbcWritableBridge.readString(67, __dbResults);
    this.OBS_Flag = JdbcWritableBridge.readString(68, __dbResults);
    this.OUT_ITEM_STTS_IND_TXT = JdbcWritableBridge.readString(69, __dbResults);
    this.PAID_AMT = JdbcWritableBridge.readDouble(70, __dbResults);
    this.PAID_SRVC_UNIT_CNT = JdbcWritableBridge.readBigDecimal(71, __dbResults);
    this.PARG_STTS_CD = JdbcWritableBridge.readString(72, __dbResults);
    this.PAYMNT_APC_NBR = JdbcWritableBridge.readString(73, __dbResults);
    this.PN_ID = JdbcWritableBridge.readString(74, __dbResults);
    this.PROD_CF_CD = JdbcWritableBridge.readString(75, __dbResults);
    this.PROD_ID = JdbcWritableBridge.readString(76, __dbResults);
    this.PROV_CITY_NM = JdbcWritableBridge.readString(77, __dbResults);
    this.PROV_ID = JdbcWritableBridge.readString(78, __dbResults);
    this.PROV_NM = JdbcWritableBridge.readString(79, __dbResults);
    this.PROV_ST_NM = JdbcWritableBridge.readString(80, __dbResults);
    this.PROV_ZIP_CD = JdbcWritableBridge.readString(81, __dbResults);
    this.RDCTN_CTGRY_CD = JdbcWritableBridge.readString(82, __dbResults);
    this.RELATIVE_WEIGHT = JdbcWritableBridge.readDouble(83, __dbResults);
    this.RELATIVE_WEIGHT1 = JdbcWritableBridge.readDouble(84, __dbResults);
    this.RNDRG_PROV_ID = JdbcWritableBridge.readString(85, __dbResults);
    this.RNDRG_PROV_ID_TYPE_CD = JdbcWritableBridge.readString(86, __dbResults);
    this.RVNU_CD = JdbcWritableBridge.readString(87, __dbResults);
    this.RVU_Flag = JdbcWritableBridge.readString(88, __dbResults);
    this.SRC_APRVL_CD = JdbcWritableBridge.readString(89, __dbResults);
    this.SRC_BILLG_TAX_ID = JdbcWritableBridge.readString(90, __dbResults);
    this.SRC_CLM_DISP_CD = JdbcWritableBridge.readString(91, __dbResults);
    this.SRC_FINDER_NBR = JdbcWritableBridge.readBigDecimal(92, __dbResults);
    this.SRC_GRP_NBR = JdbcWritableBridge.readString(93, __dbResults);
    this.SRC_HMO_CLS_CD = JdbcWritableBridge.readString(94, __dbResults);
    this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = JdbcWritableBridge.readString(95, __dbResults);
    this.SRC_INSTNL_REIMBMNT_TERM_ID = JdbcWritableBridge.readString(96, __dbResults);
    this.SRC_MEDCR_ID = JdbcWritableBridge.readString(97, __dbResults);
    this.SRC_NST_SRVC_CTGRY_CD = JdbcWritableBridge.readString(98, __dbResults);
    this.SRC_PAY_ACTN_CD = JdbcWritableBridge.readString(99, __dbResults);
    this.SRC_PRCG_CRTRIA_CD = JdbcWritableBridge.readString(100, __dbResults);
    this.SRC_PRCG_RSN_CD = JdbcWritableBridge.readString(101, __dbResults);
    this.SRC_PROV_NATL_PROV_ID = JdbcWritableBridge.readString(102, __dbResults);
    this.SRC_PRTY_SCOR_NBR = JdbcWritableBridge.readBigDecimal(103, __dbResults);
    this.SRC_SRVC_CLSFCTN_CD = JdbcWritableBridge.readString(104, __dbResults);
    this.SRC_TERM_EFCTV_DT = JdbcWritableBridge.readDate(105, __dbResults);
    this.SRC_TERM_END_DT = JdbcWritableBridge.readDate(106, __dbResults);
    this.TOS_CD = JdbcWritableBridge.readString(107, __dbResults);
    this.apcst = JdbcWritableBridge.readString(108, __dbResults);
    this.apcwt = JdbcWritableBridge.readDouble(109, __dbResults);
    this.betos = JdbcWritableBridge.readString(110, __dbResults);
    this.brand = JdbcWritableBridge.readString(111, __dbResults);
    this.cat1 = JdbcWritableBridge.readString(112, __dbResults);
    this.cat2 = JdbcWritableBridge.readString(113, __dbResults);
    this.clmwt = JdbcWritableBridge.readDouble(114, __dbResults);
    this.erwt = JdbcWritableBridge.readDouble(115, __dbResults);
    this.fundlvl2 = JdbcWritableBridge.readString(116, __dbResults);
    this.liccd = JdbcWritableBridge.readString(117, __dbResults);
    this.market = JdbcWritableBridge.readString(118, __dbResults);
    this.Inc_Month = JdbcWritableBridge.readString(119, __dbResults);
    this.network = JdbcWritableBridge.readString(120, __dbResults);
    this.normflag = JdbcWritableBridge.readString(121, __dbResults);
    this.priority = JdbcWritableBridge.readString(122, __dbResults);
    this.prodlvl3 = JdbcWritableBridge.readString(123, __dbResults);
    this.prov_county = JdbcWritableBridge.readString(124, __dbResults);
    this.surgrp = JdbcWritableBridge.readString(125, __dbResults);
    this.surgwt = JdbcWritableBridge.readDouble(126, __dbResults);
    this.system_id = JdbcWritableBridge.readString(127, __dbResults);
    this.GRP_NM = JdbcWritableBridge.readString(128, __dbResults);
    this.SRC_SUBGRP_NBR = JdbcWritableBridge.readString(129, __dbResults);
    this.VISITS = JdbcWritableBridge.readDouble(130, __dbResults);
    this.GRP_SIC = JdbcWritableBridge.readString(131, __dbResults);
    this.MCID = JdbcWritableBridge.readString(132, __dbResults);
    this.CLM_ITS_SCCF_NBR = JdbcWritableBridge.readString(133, __dbResults);
    this.MBRSHP_SOR_CD = JdbcWritableBridge.readString(134, __dbResults);
    this.SRC_RT_CTGRY_CD = JdbcWritableBridge.readString(135, __dbResults);
    this.SRC_RT_CTGRY_SUB_CD = JdbcWritableBridge.readString(136, __dbResults);
    this.run_date = JdbcWritableBridge.readDate(137, __dbResults);
    this.CLM_LINE_SRVC_STRT_DT = JdbcWritableBridge.readDate(138, __dbResults);
    this.VNDR_CD = JdbcWritableBridge.readString(139, __dbResults);
    this.REPRICE_CD = JdbcWritableBridge.readString(140, __dbResults);
    this.VNDR_PROD_CD = JdbcWritableBridge.readString(141, __dbResults);
    this.zip3 = JdbcWritableBridge.readString(142, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeDate(ADJDCTN_DT, 1 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(ADJDCTN_STTS, 2 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ADMSN_TYPE_CD, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(ADMT_DT, 4 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDouble(ALWD_AMT, 5 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(APC_MEDCR_ID, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(APC_VRSN_NBR, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BED_TYPE_CD, 8 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(BILLD_CHRG_AMT, 9 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BILLD_SRVC_UNIT_CNT, 10 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BNFT_PAYMNT_STTS_CD, 11 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CASES, 12 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(CLM_ADJSTMNT_KEY, 13 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CLM_ADJSTMNT_NBR, 14 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(CLM_DISP_CD, 15 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_ITS_HOST_CD, 16 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_LINE_NBR, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_LINE_REIMB_MTHD_CD, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_NBR, 19 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_REIMBMNT_SOR_CD, 20 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_REIMBMNT_TYPE_CD, 21 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_SOR_CD, 22 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(CLM_STMNT_FROM_DT, 23 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(CLM_STMNT_TO_DT, 24 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD, 25 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD1, 26 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD_ALLOWED, 27 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD_BILLED, 28 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CMAD_CASES, 29 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(CMPNY_CF_CD, 30 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(CVRD_EXPNS_AMT, 31 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(DERIVD_IND_CD, 32 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(DSCHRG_DT, 33 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(DSCHRG_STTS_CD, 34 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ER_Flag, 35 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PediatricFlag, 36 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ENC_Flag, 37 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EXCHNG_CERTFN_CD, 38 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EXCHNG_IND_CD, 39 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EXCHNG_METAL_TYPE_CD, 40 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(FUNDG_CF_CD, 41 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(HLTH_SRVC_CD, 42 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROC_MDFR_1_CD, 43 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROC_MDFR_2_CD, 44 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(HMO_CLS_CD, 45 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(ICL_ALWD_AMT, 46 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(ICL_PAID_AMT, 47 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(INN_CD, 48 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(LEGACY, 49 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(LINE_ITEM_APC_WT_AMT, 50 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(LINE_ITEM_PAYMNT_AMT, 51 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(MBR_County, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MBR_KEY, 53 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBR_State, 54 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBR_ZIP3, 55 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBR_ZIP_CD, 56 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBU_CF_CD, 57 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl1, 58 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl2, 59 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl3, 60 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl4, 61 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MCS, 62 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MEDCR_ID, 63 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(NRMLZD_AMT, 64 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NRMLZD_CASE, 65 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(NTWK_ID, 66 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_NTWK_KEY, 67 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(OBS_Flag, 68 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(OUT_ITEM_STTS_IND_TXT, 69 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(PAID_AMT, 70 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PAID_SRVC_UNIT_CNT, 71 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(PARG_STTS_CD, 72 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PAYMNT_APC_NBR, 73 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PN_ID, 74 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CF_CD, 75 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROD_ID, 76 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROV_CITY_NM, 77 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROV_ID, 78 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROV_NM, 79 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROV_ST_NM, 80 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROV_ZIP_CD, 81 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RDCTN_CTGRY_CD, 82 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(RELATIVE_WEIGHT, 83 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(RELATIVE_WEIGHT1, 84 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(RNDRG_PROV_ID, 85 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RNDRG_PROV_ID_TYPE_CD, 86 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RVNU_CD, 87 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RVU_Flag, 88 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_APRVL_CD, 89 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_BILLG_TAX_ID, 90 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_CLM_DISP_CD, 91 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(SRC_FINDER_NBR, 92 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(SRC_GRP_NBR, 93 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_HMO_CLS_CD, 94 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_INSTNL_NGTTD_SRVC_TERM_ID, 95 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_INSTNL_REIMBMNT_TERM_ID, 96 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_MEDCR_ID, 97 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SRC_NST_SRVC_CTGRY_CD, 98 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PAY_ACTN_CD, 99 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PRCG_CRTRIA_CD, 100 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PRCG_RSN_CD, 101 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PROV_NATL_PROV_ID, 102 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(SRC_PRTY_SCOR_NBR, 103 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(SRC_SRVC_CLSFCTN_CD, 104 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(SRC_TERM_EFCTV_DT, 105 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(SRC_TERM_END_DT, 106 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(TOS_CD, 107 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(apcst, 108 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(apcwt, 109 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(betos, 110 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(brand, 111 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(cat1, 112 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cat2, 113 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(clmwt, 114 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(erwt, 115 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(fundlvl2, 116 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(liccd, 117 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(market, 118 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(Inc_Month, 119 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(network, 120 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(normflag, 121 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(priority, 122 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(prodlvl3, 123 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(prov_county, 124 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(surgrp, 125 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(surgwt, 126 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(system_id, 127 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(GRP_NM, 128 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SRC_SUBGRP_NBR, 129 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(VISITS, 130 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(GRP_SIC, 131 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MCID, 132 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CLM_ITS_SCCF_NBR, 133 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBRSHP_SOR_CD, 134 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_RT_CTGRY_CD, 135 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_RT_CTGRY_SUB_CD, 136 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(run_date, 137 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(CLM_LINE_SRVC_STRT_DT, 138 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(VNDR_CD, 139 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REPRICE_CD, 140 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(VNDR_PROD_CD, 141 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(zip3, 142 + __off, 1, __dbStmt);
    return 142;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeDate(ADJDCTN_DT, 1 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(ADJDCTN_STTS, 2 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ADMSN_TYPE_CD, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(ADMT_DT, 4 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDouble(ALWD_AMT, 5 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(APC_MEDCR_ID, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(APC_VRSN_NBR, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BED_TYPE_CD, 8 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(BILLD_CHRG_AMT, 9 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(BILLD_SRVC_UNIT_CNT, 10 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(BNFT_PAYMNT_STTS_CD, 11 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CASES, 12 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(CLM_ADJSTMNT_KEY, 13 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CLM_ADJSTMNT_NBR, 14 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(CLM_DISP_CD, 15 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_ITS_HOST_CD, 16 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_LINE_NBR, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_LINE_REIMB_MTHD_CD, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_NBR, 19 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_REIMBMNT_SOR_CD, 20 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_REIMBMNT_TYPE_CD, 21 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_SOR_CD, 22 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(CLM_STMNT_FROM_DT, 23 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(CLM_STMNT_TO_DT, 24 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD, 25 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD1, 26 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD_ALLOWED, 27 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(CMAD_BILLED, 28 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CMAD_CASES, 29 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(CMPNY_CF_CD, 30 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(CVRD_EXPNS_AMT, 31 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(DERIVD_IND_CD, 32 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(DSCHRG_DT, 33 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(DSCHRG_STTS_CD, 34 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ER_Flag, 35 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PediatricFlag, 36 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ENC_Flag, 37 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EXCHNG_CERTFN_CD, 38 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EXCHNG_IND_CD, 39 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EXCHNG_METAL_TYPE_CD, 40 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(FUNDG_CF_CD, 41 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(HLTH_SRVC_CD, 42 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROC_MDFR_1_CD, 43 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROC_MDFR_2_CD, 44 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(HMO_CLS_CD, 45 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(ICL_ALWD_AMT, 46 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(ICL_PAID_AMT, 47 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(INN_CD, 48 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(LEGACY, 49 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(LINE_ITEM_APC_WT_AMT, 50 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(LINE_ITEM_PAYMNT_AMT, 51 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(MBR_County, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MBR_KEY, 53 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBR_State, 54 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBR_ZIP3, 55 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBR_ZIP_CD, 56 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBU_CF_CD, 57 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl1, 58 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl2, 59 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl3, 60 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MBUlvl4, 61 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MCS, 62 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MEDCR_ID, 63 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(NRMLZD_AMT, 64 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NRMLZD_CASE, 65 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(NTWK_ID, 66 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CLM_NTWK_KEY, 67 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(OBS_Flag, 68 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(OUT_ITEM_STTS_IND_TXT, 69 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(PAID_AMT, 70 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PAID_SRVC_UNIT_CNT, 71 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(PARG_STTS_CD, 72 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PAYMNT_APC_NBR, 73 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PN_ID, 74 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROD_CF_CD, 75 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROD_ID, 76 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROV_CITY_NM, 77 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROV_ID, 78 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROV_NM, 79 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PROV_ST_NM, 80 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PROV_ZIP_CD, 81 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RDCTN_CTGRY_CD, 82 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(RELATIVE_WEIGHT, 83 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(RELATIVE_WEIGHT1, 84 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(RNDRG_PROV_ID, 85 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RNDRG_PROV_ID_TYPE_CD, 86 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RVNU_CD, 87 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RVU_Flag, 88 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_APRVL_CD, 89 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_BILLG_TAX_ID, 90 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_CLM_DISP_CD, 91 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(SRC_FINDER_NBR, 92 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(SRC_GRP_NBR, 93 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_HMO_CLS_CD, 94 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_INSTNL_NGTTD_SRVC_TERM_ID, 95 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_INSTNL_REIMBMNT_TERM_ID, 96 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_MEDCR_ID, 97 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SRC_NST_SRVC_CTGRY_CD, 98 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PAY_ACTN_CD, 99 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PRCG_CRTRIA_CD, 100 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PRCG_RSN_CD, 101 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_PROV_NATL_PROV_ID, 102 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(SRC_PRTY_SCOR_NBR, 103 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(SRC_SRVC_CLSFCTN_CD, 104 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(SRC_TERM_EFCTV_DT, 105 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(SRC_TERM_END_DT, 106 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(TOS_CD, 107 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(apcst, 108 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(apcwt, 109 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(betos, 110 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(brand, 111 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(cat1, 112 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(cat2, 113 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(clmwt, 114 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeDouble(erwt, 115 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(fundlvl2, 116 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(liccd, 117 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(market, 118 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(Inc_Month, 119 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(network, 120 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(normflag, 121 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(priority, 122 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(prodlvl3, 123 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(prov_county, 124 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(surgrp, 125 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(surgwt, 126 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(system_id, 127 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(GRP_NM, 128 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SRC_SUBGRP_NBR, 129 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDouble(VISITS, 130 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeString(GRP_SIC, 131 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MCID, 132 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CLM_ITS_SCCF_NBR, 133 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MBRSHP_SOR_CD, 134 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_RT_CTGRY_CD, 135 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SRC_RT_CTGRY_SUB_CD, 136 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeDate(run_date, 137 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(CLM_LINE_SRVC_STRT_DT, 138 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(VNDR_CD, 139 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REPRICE_CD, 140 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(VNDR_PROD_CD, 141 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(zip3, 142 + __off, 1, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.ADJDCTN_DT = null;
    } else {
    this.ADJDCTN_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.ADJDCTN_STTS = null;
    } else {
    this.ADJDCTN_STTS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ADMSN_TYPE_CD = null;
    } else {
    this.ADMSN_TYPE_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ADMT_DT = null;
    } else {
    this.ADMT_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.ALWD_AMT = null;
    } else {
    this.ALWD_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.APC_MEDCR_ID = null;
    } else {
    this.APC_MEDCR_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.APC_VRSN_NBR = null;
    } else {
    this.APC_VRSN_NBR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BED_TYPE_CD = null;
    } else {
    this.BED_TYPE_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLD_CHRG_AMT = null;
    } else {
    this.BILLD_CHRG_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BILLD_SRVC_UNIT_CNT = null;
    } else {
    this.BILLD_SRVC_UNIT_CNT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.BNFT_PAYMNT_STTS_CD = null;
    } else {
    this.BNFT_PAYMNT_STTS_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CASES = null;
    } else {
    this.CASES = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_ADJSTMNT_KEY = null;
    } else {
    this.CLM_ADJSTMNT_KEY = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_ADJSTMNT_NBR = null;
    } else {
    this.CLM_ADJSTMNT_NBR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_DISP_CD = null;
    } else {
    this.CLM_DISP_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_ITS_HOST_CD = null;
    } else {
    this.CLM_ITS_HOST_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_LINE_NBR = null;
    } else {
    this.CLM_LINE_NBR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_LINE_REIMB_MTHD_CD = null;
    } else {
    this.CLM_LINE_REIMB_MTHD_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_NBR = null;
    } else {
    this.CLM_NBR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_REIMBMNT_SOR_CD = null;
    } else {
    this.CLM_REIMBMNT_SOR_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_REIMBMNT_TYPE_CD = null;
    } else {
    this.CLM_REIMBMNT_TYPE_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_SOR_CD = null;
    } else {
    this.CLM_SOR_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_STMNT_FROM_DT = null;
    } else {
    this.CLM_STMNT_FROM_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_STMNT_TO_DT = null;
    } else {
    this.CLM_STMNT_TO_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.CMAD = null;
    } else {
    this.CMAD = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CMAD1 = null;
    } else {
    this.CMAD1 = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CMAD_ALLOWED = null;
    } else {
    this.CMAD_ALLOWED = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CMAD_BILLED = null;
    } else {
    this.CMAD_BILLED = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.CMAD_CASES = null;
    } else {
    this.CMAD_CASES = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CMPNY_CF_CD = null;
    } else {
    this.CMPNY_CF_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CVRD_EXPNS_AMT = null;
    } else {
    this.CVRD_EXPNS_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.DERIVD_IND_CD = null;
    } else {
    this.DERIVD_IND_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DSCHRG_DT = null;
    } else {
    this.DSCHRG_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.DSCHRG_STTS_CD = null;
    } else {
    this.DSCHRG_STTS_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ER_Flag = null;
    } else {
    this.ER_Flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PediatricFlag = null;
    } else {
    this.PediatricFlag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ENC_Flag = null;
    } else {
    this.ENC_Flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EXCHNG_CERTFN_CD = null;
    } else {
    this.EXCHNG_CERTFN_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EXCHNG_IND_CD = null;
    } else {
    this.EXCHNG_IND_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EXCHNG_METAL_TYPE_CD = null;
    } else {
    this.EXCHNG_METAL_TYPE_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FUNDG_CF_CD = null;
    } else {
    this.FUNDG_CF_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.HLTH_SRVC_CD = null;
    } else {
    this.HLTH_SRVC_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROC_MDFR_1_CD = null;
    } else {
    this.PROC_MDFR_1_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROC_MDFR_2_CD = null;
    } else {
    this.PROC_MDFR_2_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.HMO_CLS_CD = null;
    } else {
    this.HMO_CLS_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ICL_ALWD_AMT = null;
    } else {
    this.ICL_ALWD_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.ICL_PAID_AMT = null;
    } else {
    this.ICL_PAID_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.INN_CD = null;
    } else {
    this.INN_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.LEGACY = null;
    } else {
    this.LEGACY = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.LINE_ITEM_APC_WT_AMT = null;
    } else {
    this.LINE_ITEM_APC_WT_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.LINE_ITEM_PAYMNT_AMT = null;
    } else {
    this.LINE_ITEM_PAYMNT_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.MBR_County = null;
    } else {
    this.MBR_County = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBR_KEY = null;
    } else {
    this.MBR_KEY = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBR_State = null;
    } else {
    this.MBR_State = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBR_ZIP3 = null;
    } else {
    this.MBR_ZIP3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBR_ZIP_CD = null;
    } else {
    this.MBR_ZIP_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBU_CF_CD = null;
    } else {
    this.MBU_CF_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBUlvl1 = null;
    } else {
    this.MBUlvl1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBUlvl2 = null;
    } else {
    this.MBUlvl2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBUlvl3 = null;
    } else {
    this.MBUlvl3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBUlvl4 = null;
    } else {
    this.MBUlvl4 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MCS = null;
    } else {
    this.MCS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MEDCR_ID = null;
    } else {
    this.MEDCR_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NRMLZD_AMT = null;
    } else {
    this.NRMLZD_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.NRMLZD_CASE = null;
    } else {
    this.NRMLZD_CASE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NTWK_ID = null;
    } else {
    this.NTWK_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_NTWK_KEY = null;
    } else {
    this.CLM_NTWK_KEY = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OBS_Flag = null;
    } else {
    this.OBS_Flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OUT_ITEM_STTS_IND_TXT = null;
    } else {
    this.OUT_ITEM_STTS_IND_TXT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PAID_AMT = null;
    } else {
    this.PAID_AMT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.PAID_SRVC_UNIT_CNT = null;
    } else {
    this.PAID_SRVC_UNIT_CNT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PARG_STTS_CD = null;
    } else {
    this.PARG_STTS_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PAYMNT_APC_NBR = null;
    } else {
    this.PAYMNT_APC_NBR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PN_ID = null;
    } else {
    this.PN_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_CF_CD = null;
    } else {
    this.PROD_CF_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROD_ID = null;
    } else {
    this.PROD_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROV_CITY_NM = null;
    } else {
    this.PROV_CITY_NM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROV_ID = null;
    } else {
    this.PROV_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROV_NM = null;
    } else {
    this.PROV_NM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROV_ST_NM = null;
    } else {
    this.PROV_ST_NM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROV_ZIP_CD = null;
    } else {
    this.PROV_ZIP_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RDCTN_CTGRY_CD = null;
    } else {
    this.RDCTN_CTGRY_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RELATIVE_WEIGHT = null;
    } else {
    this.RELATIVE_WEIGHT = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.RELATIVE_WEIGHT1 = null;
    } else {
    this.RELATIVE_WEIGHT1 = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.RNDRG_PROV_ID = null;
    } else {
    this.RNDRG_PROV_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RNDRG_PROV_ID_TYPE_CD = null;
    } else {
    this.RNDRG_PROV_ID_TYPE_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RVNU_CD = null;
    } else {
    this.RVNU_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RVU_Flag = null;
    } else {
    this.RVU_Flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_APRVL_CD = null;
    } else {
    this.SRC_APRVL_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_BILLG_TAX_ID = null;
    } else {
    this.SRC_BILLG_TAX_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_CLM_DISP_CD = null;
    } else {
    this.SRC_CLM_DISP_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_FINDER_NBR = null;
    } else {
    this.SRC_FINDER_NBR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_GRP_NBR = null;
    } else {
    this.SRC_GRP_NBR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_HMO_CLS_CD = null;
    } else {
    this.SRC_HMO_CLS_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = null;
    } else {
    this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_INSTNL_REIMBMNT_TERM_ID = null;
    } else {
    this.SRC_INSTNL_REIMBMNT_TERM_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_MEDCR_ID = null;
    } else {
    this.SRC_MEDCR_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_NST_SRVC_CTGRY_CD = null;
    } else {
    this.SRC_NST_SRVC_CTGRY_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_PAY_ACTN_CD = null;
    } else {
    this.SRC_PAY_ACTN_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_PRCG_CRTRIA_CD = null;
    } else {
    this.SRC_PRCG_CRTRIA_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_PRCG_RSN_CD = null;
    } else {
    this.SRC_PRCG_RSN_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_PROV_NATL_PROV_ID = null;
    } else {
    this.SRC_PROV_NATL_PROV_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_PRTY_SCOR_NBR = null;
    } else {
    this.SRC_PRTY_SCOR_NBR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_SRVC_CLSFCTN_CD = null;
    } else {
    this.SRC_SRVC_CLSFCTN_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_TERM_EFCTV_DT = null;
    } else {
    this.SRC_TERM_EFCTV_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_TERM_END_DT = null;
    } else {
    this.SRC_TERM_END_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.TOS_CD = null;
    } else {
    this.TOS_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.apcst = null;
    } else {
    this.apcst = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.apcwt = null;
    } else {
    this.apcwt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.betos = null;
    } else {
    this.betos = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.brand = null;
    } else {
    this.brand = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cat1 = null;
    } else {
    this.cat1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.cat2 = null;
    } else {
    this.cat2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.clmwt = null;
    } else {
    this.clmwt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.erwt = null;
    } else {
    this.erwt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.fundlvl2 = null;
    } else {
    this.fundlvl2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.liccd = null;
    } else {
    this.liccd = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.market = null;
    } else {
    this.market = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Inc_Month = null;
    } else {
    this.Inc_Month = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.network = null;
    } else {
    this.network = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.normflag = null;
    } else {
    this.normflag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.priority = null;
    } else {
    this.priority = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.prodlvl3 = null;
    } else {
    this.prodlvl3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.prov_county = null;
    } else {
    this.prov_county = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.surgrp = null;
    } else {
    this.surgrp = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.surgwt = null;
    } else {
    this.surgwt = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.system_id = null;
    } else {
    this.system_id = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.GRP_NM = null;
    } else {
    this.GRP_NM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_SUBGRP_NBR = null;
    } else {
    this.SRC_SUBGRP_NBR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.VISITS = null;
    } else {
    this.VISITS = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.GRP_SIC = null;
    } else {
    this.GRP_SIC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MCID = null;
    } else {
    this.MCID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_ITS_SCCF_NBR = null;
    } else {
    this.CLM_ITS_SCCF_NBR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MBRSHP_SOR_CD = null;
    } else {
    this.MBRSHP_SOR_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_RT_CTGRY_CD = null;
    } else {
    this.SRC_RT_CTGRY_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SRC_RT_CTGRY_SUB_CD = null;
    } else {
    this.SRC_RT_CTGRY_SUB_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.run_date = null;
    } else {
    this.run_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.CLM_LINE_SRVC_STRT_DT = null;
    } else {
    this.CLM_LINE_SRVC_STRT_DT = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.VNDR_CD = null;
    } else {
    this.VNDR_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REPRICE_CD = null;
    } else {
    this.REPRICE_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.VNDR_PROD_CD = null;
    } else {
    this.VNDR_PROD_CD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.zip3 = null;
    } else {
    this.zip3 = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.ADJDCTN_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.ADJDCTN_DT.getTime());
    }
    if (null == this.ADJDCTN_STTS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ADJDCTN_STTS);
    }
    if (null == this.ADMSN_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ADMSN_TYPE_CD);
    }
    if (null == this.ADMT_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.ADMT_DT.getTime());
    }
    if (null == this.ALWD_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ALWD_AMT);
    }
    if (null == this.APC_MEDCR_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, APC_MEDCR_ID);
    }
    if (null == this.APC_VRSN_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, APC_VRSN_NBR);
    }
    if (null == this.BED_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BED_TYPE_CD);
    }
    if (null == this.BILLD_CHRG_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BILLD_CHRG_AMT);
    }
    if (null == this.BILLD_SRVC_UNIT_CNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BILLD_SRVC_UNIT_CNT);
    }
    if (null == this.BNFT_PAYMNT_STTS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNFT_PAYMNT_STTS_CD);
    }
    if (null == this.CASES) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CASES, __dataOut);
    }
    if (null == this.CLM_ADJSTMNT_KEY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_ADJSTMNT_KEY);
    }
    if (null == this.CLM_ADJSTMNT_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CLM_ADJSTMNT_NBR, __dataOut);
    }
    if (null == this.CLM_DISP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_DISP_CD);
    }
    if (null == this.CLM_ITS_HOST_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_ITS_HOST_CD);
    }
    if (null == this.CLM_LINE_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_LINE_NBR);
    }
    if (null == this.CLM_LINE_REIMB_MTHD_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_LINE_REIMB_MTHD_CD);
    }
    if (null == this.CLM_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_NBR);
    }
    if (null == this.CLM_REIMBMNT_SOR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_REIMBMNT_SOR_CD);
    }
    if (null == this.CLM_REIMBMNT_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_REIMBMNT_TYPE_CD);
    }
    if (null == this.CLM_SOR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_SOR_CD);
    }
    if (null == this.CLM_STMNT_FROM_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.CLM_STMNT_FROM_DT.getTime());
    }
    if (null == this.CLM_STMNT_TO_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.CLM_STMNT_TO_DT.getTime());
    }
    if (null == this.CMAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD);
    }
    if (null == this.CMAD1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD1);
    }
    if (null == this.CMAD_ALLOWED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD_ALLOWED);
    }
    if (null == this.CMAD_BILLED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD_BILLED);
    }
    if (null == this.CMAD_CASES) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CMAD_CASES, __dataOut);
    }
    if (null == this.CMPNY_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CMPNY_CF_CD);
    }
    if (null == this.CVRD_EXPNS_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CVRD_EXPNS_AMT);
    }
    if (null == this.DERIVD_IND_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DERIVD_IND_CD);
    }
    if (null == this.DSCHRG_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.DSCHRG_DT.getTime());
    }
    if (null == this.DSCHRG_STTS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DSCHRG_STTS_CD);
    }
    if (null == this.ER_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ER_Flag);
    }
    if (null == this.PediatricFlag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PediatricFlag);
    }
    if (null == this.ENC_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ENC_Flag);
    }
    if (null == this.EXCHNG_CERTFN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EXCHNG_CERTFN_CD);
    }
    if (null == this.EXCHNG_IND_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EXCHNG_IND_CD);
    }
    if (null == this.EXCHNG_METAL_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EXCHNG_METAL_TYPE_CD);
    }
    if (null == this.FUNDG_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FUNDG_CF_CD);
    }
    if (null == this.HLTH_SRVC_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, HLTH_SRVC_CD);
    }
    if (null == this.PROC_MDFR_1_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROC_MDFR_1_CD);
    }
    if (null == this.PROC_MDFR_2_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROC_MDFR_2_CD);
    }
    if (null == this.HMO_CLS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, HMO_CLS_CD);
    }
    if (null == this.ICL_ALWD_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ICL_ALWD_AMT);
    }
    if (null == this.ICL_PAID_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ICL_PAID_AMT);
    }
    if (null == this.INN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INN_CD);
    }
    if (null == this.LEGACY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LEGACY);
    }
    if (null == this.LINE_ITEM_APC_WT_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.LINE_ITEM_APC_WT_AMT);
    }
    if (null == this.LINE_ITEM_PAYMNT_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.LINE_ITEM_PAYMNT_AMT);
    }
    if (null == this.MBR_County) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_County);
    }
    if (null == this.MBR_KEY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_KEY);
    }
    if (null == this.MBR_State) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_State);
    }
    if (null == this.MBR_ZIP3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_ZIP3);
    }
    if (null == this.MBR_ZIP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_ZIP_CD);
    }
    if (null == this.MBU_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBU_CF_CD);
    }
    if (null == this.MBUlvl1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl1);
    }
    if (null == this.MBUlvl2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl2);
    }
    if (null == this.MBUlvl3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl3);
    }
    if (null == this.MBUlvl4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl4);
    }
    if (null == this.MCS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MCS);
    }
    if (null == this.MEDCR_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MEDCR_ID);
    }
    if (null == this.NRMLZD_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.NRMLZD_AMT);
    }
    if (null == this.NRMLZD_CASE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NRMLZD_CASE, __dataOut);
    }
    if (null == this.NTWK_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NTWK_ID);
    }
    if (null == this.CLM_NTWK_KEY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_NTWK_KEY);
    }
    if (null == this.OBS_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OBS_Flag);
    }
    if (null == this.OUT_ITEM_STTS_IND_TXT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUT_ITEM_STTS_IND_TXT);
    }
    if (null == this.PAID_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PAID_AMT);
    }
    if (null == this.PAID_SRVC_UNIT_CNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PAID_SRVC_UNIT_CNT, __dataOut);
    }
    if (null == this.PARG_STTS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PARG_STTS_CD);
    }
    if (null == this.PAYMNT_APC_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PAYMNT_APC_NBR);
    }
    if (null == this.PN_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PN_ID);
    }
    if (null == this.PROD_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CF_CD);
    }
    if (null == this.PROD_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_ID);
    }
    if (null == this.PROV_CITY_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_CITY_NM);
    }
    if (null == this.PROV_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_ID);
    }
    if (null == this.PROV_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_NM);
    }
    if (null == this.PROV_ST_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_ST_NM);
    }
    if (null == this.PROV_ZIP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_ZIP_CD);
    }
    if (null == this.RDCTN_CTGRY_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RDCTN_CTGRY_CD);
    }
    if (null == this.RELATIVE_WEIGHT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RELATIVE_WEIGHT);
    }
    if (null == this.RELATIVE_WEIGHT1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RELATIVE_WEIGHT1);
    }
    if (null == this.RNDRG_PROV_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RNDRG_PROV_ID);
    }
    if (null == this.RNDRG_PROV_ID_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RNDRG_PROV_ID_TYPE_CD);
    }
    if (null == this.RVNU_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RVNU_CD);
    }
    if (null == this.RVU_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RVU_Flag);
    }
    if (null == this.SRC_APRVL_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_APRVL_CD);
    }
    if (null == this.SRC_BILLG_TAX_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_BILLG_TAX_ID);
    }
    if (null == this.SRC_CLM_DISP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_CLM_DISP_CD);
    }
    if (null == this.SRC_FINDER_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.SRC_FINDER_NBR, __dataOut);
    }
    if (null == this.SRC_GRP_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_GRP_NBR);
    }
    if (null == this.SRC_HMO_CLS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_HMO_CLS_CD);
    }
    if (null == this.SRC_INSTNL_NGTTD_SRVC_TERM_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_INSTNL_NGTTD_SRVC_TERM_ID);
    }
    if (null == this.SRC_INSTNL_REIMBMNT_TERM_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_INSTNL_REIMBMNT_TERM_ID);
    }
    if (null == this.SRC_MEDCR_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_MEDCR_ID);
    }
    if (null == this.SRC_NST_SRVC_CTGRY_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_NST_SRVC_CTGRY_CD);
    }
    if (null == this.SRC_PAY_ACTN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PAY_ACTN_CD);
    }
    if (null == this.SRC_PRCG_CRTRIA_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PRCG_CRTRIA_CD);
    }
    if (null == this.SRC_PRCG_RSN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PRCG_RSN_CD);
    }
    if (null == this.SRC_PROV_NATL_PROV_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PROV_NATL_PROV_ID);
    }
    if (null == this.SRC_PRTY_SCOR_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.SRC_PRTY_SCOR_NBR, __dataOut);
    }
    if (null == this.SRC_SRVC_CLSFCTN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_SRVC_CLSFCTN_CD);
    }
    if (null == this.SRC_TERM_EFCTV_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.SRC_TERM_EFCTV_DT.getTime());
    }
    if (null == this.SRC_TERM_END_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.SRC_TERM_END_DT.getTime());
    }
    if (null == this.TOS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TOS_CD);
    }
    if (null == this.apcst) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, apcst);
    }
    if (null == this.apcwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.apcwt);
    }
    if (null == this.betos) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, betos);
    }
    if (null == this.brand) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, brand);
    }
    if (null == this.cat1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cat1);
    }
    if (null == this.cat2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cat2);
    }
    if (null == this.clmwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.clmwt);
    }
    if (null == this.erwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.erwt);
    }
    if (null == this.fundlvl2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, fundlvl2);
    }
    if (null == this.liccd) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, liccd);
    }
    if (null == this.market) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, market);
    }
    if (null == this.Inc_Month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Inc_Month);
    }
    if (null == this.network) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, network);
    }
    if (null == this.normflag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, normflag);
    }
    if (null == this.priority) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, priority);
    }
    if (null == this.prodlvl3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, prodlvl3);
    }
    if (null == this.prov_county) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, prov_county);
    }
    if (null == this.surgrp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, surgrp);
    }
    if (null == this.surgwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.surgwt);
    }
    if (null == this.system_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, system_id);
    }
    if (null == this.GRP_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRP_NM);
    }
    if (null == this.SRC_SUBGRP_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_SUBGRP_NBR);
    }
    if (null == this.VISITS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.VISITS);
    }
    if (null == this.GRP_SIC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRP_SIC);
    }
    if (null == this.MCID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MCID);
    }
    if (null == this.CLM_ITS_SCCF_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_ITS_SCCF_NBR);
    }
    if (null == this.MBRSHP_SOR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBRSHP_SOR_CD);
    }
    if (null == this.SRC_RT_CTGRY_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_RT_CTGRY_CD);
    }
    if (null == this.SRC_RT_CTGRY_SUB_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_RT_CTGRY_SUB_CD);
    }
    if (null == this.run_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.run_date.getTime());
    }
    if (null == this.CLM_LINE_SRVC_STRT_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.CLM_LINE_SRVC_STRT_DT.getTime());
    }
    if (null == this.VNDR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, VNDR_CD);
    }
    if (null == this.REPRICE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REPRICE_CD);
    }
    if (null == this.VNDR_PROD_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, VNDR_PROD_CD);
    }
    if (null == this.zip3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zip3);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.ADJDCTN_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.ADJDCTN_DT.getTime());
    }
    if (null == this.ADJDCTN_STTS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ADJDCTN_STTS);
    }
    if (null == this.ADMSN_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ADMSN_TYPE_CD);
    }
    if (null == this.ADMT_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.ADMT_DT.getTime());
    }
    if (null == this.ALWD_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ALWD_AMT);
    }
    if (null == this.APC_MEDCR_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, APC_MEDCR_ID);
    }
    if (null == this.APC_VRSN_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, APC_VRSN_NBR);
    }
    if (null == this.BED_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BED_TYPE_CD);
    }
    if (null == this.BILLD_CHRG_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BILLD_CHRG_AMT);
    }
    if (null == this.BILLD_SRVC_UNIT_CNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.BILLD_SRVC_UNIT_CNT);
    }
    if (null == this.BNFT_PAYMNT_STTS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNFT_PAYMNT_STTS_CD);
    }
    if (null == this.CASES) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CASES, __dataOut);
    }
    if (null == this.CLM_ADJSTMNT_KEY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_ADJSTMNT_KEY);
    }
    if (null == this.CLM_ADJSTMNT_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CLM_ADJSTMNT_NBR, __dataOut);
    }
    if (null == this.CLM_DISP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_DISP_CD);
    }
    if (null == this.CLM_ITS_HOST_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_ITS_HOST_CD);
    }
    if (null == this.CLM_LINE_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_LINE_NBR);
    }
    if (null == this.CLM_LINE_REIMB_MTHD_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_LINE_REIMB_MTHD_CD);
    }
    if (null == this.CLM_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_NBR);
    }
    if (null == this.CLM_REIMBMNT_SOR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_REIMBMNT_SOR_CD);
    }
    if (null == this.CLM_REIMBMNT_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_REIMBMNT_TYPE_CD);
    }
    if (null == this.CLM_SOR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_SOR_CD);
    }
    if (null == this.CLM_STMNT_FROM_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.CLM_STMNT_FROM_DT.getTime());
    }
    if (null == this.CLM_STMNT_TO_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.CLM_STMNT_TO_DT.getTime());
    }
    if (null == this.CMAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD);
    }
    if (null == this.CMAD1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD1);
    }
    if (null == this.CMAD_ALLOWED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD_ALLOWED);
    }
    if (null == this.CMAD_BILLED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CMAD_BILLED);
    }
    if (null == this.CMAD_CASES) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CMAD_CASES, __dataOut);
    }
    if (null == this.CMPNY_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CMPNY_CF_CD);
    }
    if (null == this.CVRD_EXPNS_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.CVRD_EXPNS_AMT);
    }
    if (null == this.DERIVD_IND_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DERIVD_IND_CD);
    }
    if (null == this.DSCHRG_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.DSCHRG_DT.getTime());
    }
    if (null == this.DSCHRG_STTS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DSCHRG_STTS_CD);
    }
    if (null == this.ER_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ER_Flag);
    }
    if (null == this.PediatricFlag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PediatricFlag);
    }
    if (null == this.ENC_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ENC_Flag);
    }
    if (null == this.EXCHNG_CERTFN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EXCHNG_CERTFN_CD);
    }
    if (null == this.EXCHNG_IND_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EXCHNG_IND_CD);
    }
    if (null == this.EXCHNG_METAL_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EXCHNG_METAL_TYPE_CD);
    }
    if (null == this.FUNDG_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FUNDG_CF_CD);
    }
    if (null == this.HLTH_SRVC_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, HLTH_SRVC_CD);
    }
    if (null == this.PROC_MDFR_1_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROC_MDFR_1_CD);
    }
    if (null == this.PROC_MDFR_2_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROC_MDFR_2_CD);
    }
    if (null == this.HMO_CLS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, HMO_CLS_CD);
    }
    if (null == this.ICL_ALWD_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ICL_ALWD_AMT);
    }
    if (null == this.ICL_PAID_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.ICL_PAID_AMT);
    }
    if (null == this.INN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INN_CD);
    }
    if (null == this.LEGACY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LEGACY);
    }
    if (null == this.LINE_ITEM_APC_WT_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.LINE_ITEM_APC_WT_AMT);
    }
    if (null == this.LINE_ITEM_PAYMNT_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.LINE_ITEM_PAYMNT_AMT);
    }
    if (null == this.MBR_County) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_County);
    }
    if (null == this.MBR_KEY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_KEY);
    }
    if (null == this.MBR_State) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_State);
    }
    if (null == this.MBR_ZIP3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_ZIP3);
    }
    if (null == this.MBR_ZIP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBR_ZIP_CD);
    }
    if (null == this.MBU_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBU_CF_CD);
    }
    if (null == this.MBUlvl1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl1);
    }
    if (null == this.MBUlvl2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl2);
    }
    if (null == this.MBUlvl3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl3);
    }
    if (null == this.MBUlvl4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBUlvl4);
    }
    if (null == this.MCS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MCS);
    }
    if (null == this.MEDCR_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MEDCR_ID);
    }
    if (null == this.NRMLZD_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.NRMLZD_AMT);
    }
    if (null == this.NRMLZD_CASE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NRMLZD_CASE, __dataOut);
    }
    if (null == this.NTWK_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NTWK_ID);
    }
    if (null == this.CLM_NTWK_KEY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_NTWK_KEY);
    }
    if (null == this.OBS_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OBS_Flag);
    }
    if (null == this.OUT_ITEM_STTS_IND_TXT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUT_ITEM_STTS_IND_TXT);
    }
    if (null == this.PAID_AMT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.PAID_AMT);
    }
    if (null == this.PAID_SRVC_UNIT_CNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PAID_SRVC_UNIT_CNT, __dataOut);
    }
    if (null == this.PARG_STTS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PARG_STTS_CD);
    }
    if (null == this.PAYMNT_APC_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PAYMNT_APC_NBR);
    }
    if (null == this.PN_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PN_ID);
    }
    if (null == this.PROD_CF_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_CF_CD);
    }
    if (null == this.PROD_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROD_ID);
    }
    if (null == this.PROV_CITY_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_CITY_NM);
    }
    if (null == this.PROV_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_ID);
    }
    if (null == this.PROV_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_NM);
    }
    if (null == this.PROV_ST_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_ST_NM);
    }
    if (null == this.PROV_ZIP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROV_ZIP_CD);
    }
    if (null == this.RDCTN_CTGRY_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RDCTN_CTGRY_CD);
    }
    if (null == this.RELATIVE_WEIGHT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RELATIVE_WEIGHT);
    }
    if (null == this.RELATIVE_WEIGHT1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.RELATIVE_WEIGHT1);
    }
    if (null == this.RNDRG_PROV_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RNDRG_PROV_ID);
    }
    if (null == this.RNDRG_PROV_ID_TYPE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RNDRG_PROV_ID_TYPE_CD);
    }
    if (null == this.RVNU_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RVNU_CD);
    }
    if (null == this.RVU_Flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RVU_Flag);
    }
    if (null == this.SRC_APRVL_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_APRVL_CD);
    }
    if (null == this.SRC_BILLG_TAX_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_BILLG_TAX_ID);
    }
    if (null == this.SRC_CLM_DISP_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_CLM_DISP_CD);
    }
    if (null == this.SRC_FINDER_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.SRC_FINDER_NBR, __dataOut);
    }
    if (null == this.SRC_GRP_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_GRP_NBR);
    }
    if (null == this.SRC_HMO_CLS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_HMO_CLS_CD);
    }
    if (null == this.SRC_INSTNL_NGTTD_SRVC_TERM_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_INSTNL_NGTTD_SRVC_TERM_ID);
    }
    if (null == this.SRC_INSTNL_REIMBMNT_TERM_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_INSTNL_REIMBMNT_TERM_ID);
    }
    if (null == this.SRC_MEDCR_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_MEDCR_ID);
    }
    if (null == this.SRC_NST_SRVC_CTGRY_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_NST_SRVC_CTGRY_CD);
    }
    if (null == this.SRC_PAY_ACTN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PAY_ACTN_CD);
    }
    if (null == this.SRC_PRCG_CRTRIA_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PRCG_CRTRIA_CD);
    }
    if (null == this.SRC_PRCG_RSN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PRCG_RSN_CD);
    }
    if (null == this.SRC_PROV_NATL_PROV_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_PROV_NATL_PROV_ID);
    }
    if (null == this.SRC_PRTY_SCOR_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.SRC_PRTY_SCOR_NBR, __dataOut);
    }
    if (null == this.SRC_SRVC_CLSFCTN_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_SRVC_CLSFCTN_CD);
    }
    if (null == this.SRC_TERM_EFCTV_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.SRC_TERM_EFCTV_DT.getTime());
    }
    if (null == this.SRC_TERM_END_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.SRC_TERM_END_DT.getTime());
    }
    if (null == this.TOS_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TOS_CD);
    }
    if (null == this.apcst) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, apcst);
    }
    if (null == this.apcwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.apcwt);
    }
    if (null == this.betos) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, betos);
    }
    if (null == this.brand) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, brand);
    }
    if (null == this.cat1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cat1);
    }
    if (null == this.cat2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, cat2);
    }
    if (null == this.clmwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.clmwt);
    }
    if (null == this.erwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.erwt);
    }
    if (null == this.fundlvl2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, fundlvl2);
    }
    if (null == this.liccd) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, liccd);
    }
    if (null == this.market) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, market);
    }
    if (null == this.Inc_Month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Inc_Month);
    }
    if (null == this.network) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, network);
    }
    if (null == this.normflag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, normflag);
    }
    if (null == this.priority) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, priority);
    }
    if (null == this.prodlvl3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, prodlvl3);
    }
    if (null == this.prov_county) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, prov_county);
    }
    if (null == this.surgrp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, surgrp);
    }
    if (null == this.surgwt) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.surgwt);
    }
    if (null == this.system_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, system_id);
    }
    if (null == this.GRP_NM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRP_NM);
    }
    if (null == this.SRC_SUBGRP_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_SUBGRP_NBR);
    }
    if (null == this.VISITS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.VISITS);
    }
    if (null == this.GRP_SIC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRP_SIC);
    }
    if (null == this.MCID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MCID);
    }
    if (null == this.CLM_ITS_SCCF_NBR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLM_ITS_SCCF_NBR);
    }
    if (null == this.MBRSHP_SOR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MBRSHP_SOR_CD);
    }
    if (null == this.SRC_RT_CTGRY_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_RT_CTGRY_CD);
    }
    if (null == this.SRC_RT_CTGRY_SUB_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SRC_RT_CTGRY_SUB_CD);
    }
    if (null == this.run_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.run_date.getTime());
    }
    if (null == this.CLM_LINE_SRVC_STRT_DT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.CLM_LINE_SRVC_STRT_DT.getTime());
    }
    if (null == this.VNDR_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, VNDR_CD);
    }
    if (null == this.REPRICE_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REPRICE_CD);
    }
    if (null == this.VNDR_PROD_CD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, VNDR_PROD_CD);
    }
    if (null == this.zip3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zip3);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(ADJDCTN_DT==null?"null":"" + ADJDCTN_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADJDCTN_STTS==null?"null":ADJDCTN_STTS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADMSN_TYPE_CD==null?"null":ADMSN_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADMT_DT==null?"null":"" + ADMT_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ALWD_AMT==null?"null":"" + ALWD_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APC_MEDCR_ID==null?"null":APC_MEDCR_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APC_VRSN_NBR==null?"null":APC_VRSN_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BED_TYPE_CD==null?"null":BED_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLD_CHRG_AMT==null?"null":"" + BILLD_CHRG_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLD_SRVC_UNIT_CNT==null?"null":"" + BILLD_SRVC_UNIT_CNT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNFT_PAYMNT_STTS_CD==null?"null":BNFT_PAYMNT_STTS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CASES==null?"null":CASES.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ADJSTMNT_KEY==null?"null":CLM_ADJSTMNT_KEY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ADJSTMNT_NBR==null?"null":CLM_ADJSTMNT_NBR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_DISP_CD==null?"null":CLM_DISP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ITS_HOST_CD==null?"null":CLM_ITS_HOST_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_LINE_NBR==null?"null":CLM_LINE_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_LINE_REIMB_MTHD_CD==null?"null":CLM_LINE_REIMB_MTHD_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_NBR==null?"null":CLM_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_REIMBMNT_SOR_CD==null?"null":CLM_REIMBMNT_SOR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_REIMBMNT_TYPE_CD==null?"null":CLM_REIMBMNT_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_SOR_CD==null?"null":CLM_SOR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_STMNT_FROM_DT==null?"null":"" + CLM_STMNT_FROM_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_STMNT_TO_DT==null?"null":"" + CLM_STMNT_TO_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD==null?"null":"" + CMAD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD1==null?"null":"" + CMAD1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD_ALLOWED==null?"null":"" + CMAD_ALLOWED, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD_BILLED==null?"null":"" + CMAD_BILLED, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD_CASES==null?"null":CMAD_CASES.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMPNY_CF_CD==null?"null":CMPNY_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CVRD_EXPNS_AMT==null?"null":"" + CVRD_EXPNS_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DERIVD_IND_CD==null?"null":DERIVD_IND_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DSCHRG_DT==null?"null":"" + DSCHRG_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DSCHRG_STTS_CD==null?"null":DSCHRG_STTS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ER_Flag==null?"null":ER_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PediatricFlag==null?"null":PediatricFlag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ENC_Flag==null?"null":ENC_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EXCHNG_CERTFN_CD==null?"null":EXCHNG_CERTFN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EXCHNG_IND_CD==null?"null":EXCHNG_IND_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EXCHNG_METAL_TYPE_CD==null?"null":EXCHNG_METAL_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FUNDG_CF_CD==null?"null":FUNDG_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(HLTH_SRVC_CD==null?"null":HLTH_SRVC_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROC_MDFR_1_CD==null?"null":PROC_MDFR_1_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROC_MDFR_2_CD==null?"null":PROC_MDFR_2_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(HMO_CLS_CD==null?"null":HMO_CLS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ICL_ALWD_AMT==null?"null":"" + ICL_ALWD_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ICL_PAID_AMT==null?"null":"" + ICL_PAID_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INN_CD==null?"null":INN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LEGACY==null?"null":LEGACY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LINE_ITEM_APC_WT_AMT==null?"null":"" + LINE_ITEM_APC_WT_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LINE_ITEM_PAYMNT_AMT==null?"null":"" + LINE_ITEM_PAYMNT_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_County==null?"null":MBR_County, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_KEY==null?"null":MBR_KEY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_State==null?"null":MBR_State, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_ZIP3==null?"null":MBR_ZIP3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_ZIP_CD==null?"null":MBR_ZIP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBU_CF_CD==null?"null":MBU_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl1==null?"null":MBUlvl1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl2==null?"null":MBUlvl2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl3==null?"null":MBUlvl3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl4==null?"null":MBUlvl4, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MCS==null?"null":MCS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MEDCR_ID==null?"null":MEDCR_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NRMLZD_AMT==null?"null":"" + NRMLZD_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NRMLZD_CASE==null?"null":NRMLZD_CASE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NTWK_ID==null?"null":NTWK_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_NTWK_KEY==null?"null":CLM_NTWK_KEY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OBS_Flag==null?"null":OBS_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUT_ITEM_STTS_IND_TXT==null?"null":OUT_ITEM_STTS_IND_TXT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PAID_AMT==null?"null":"" + PAID_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PAID_SRVC_UNIT_CNT==null?"null":PAID_SRVC_UNIT_CNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PARG_STTS_CD==null?"null":PARG_STTS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PAYMNT_APC_NBR==null?"null":PAYMNT_APC_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PN_ID==null?"null":PN_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CF_CD==null?"null":PROD_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_ID==null?"null":PROD_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_CITY_NM==null?"null":PROV_CITY_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_ID==null?"null":PROV_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_NM==null?"null":PROV_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_ST_NM==null?"null":PROV_ST_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_ZIP_CD==null?"null":PROV_ZIP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RDCTN_CTGRY_CD==null?"null":RDCTN_CTGRY_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RELATIVE_WEIGHT==null?"null":"" + RELATIVE_WEIGHT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RELATIVE_WEIGHT1==null?"null":"" + RELATIVE_WEIGHT1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RNDRG_PROV_ID==null?"null":RNDRG_PROV_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RNDRG_PROV_ID_TYPE_CD==null?"null":RNDRG_PROV_ID_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RVNU_CD==null?"null":RVNU_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RVU_Flag==null?"null":RVU_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_APRVL_CD==null?"null":SRC_APRVL_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_BILLG_TAX_ID==null?"null":SRC_BILLG_TAX_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_CLM_DISP_CD==null?"null":SRC_CLM_DISP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_FINDER_NBR==null?"null":SRC_FINDER_NBR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_GRP_NBR==null?"null":SRC_GRP_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_HMO_CLS_CD==null?"null":SRC_HMO_CLS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_INSTNL_NGTTD_SRVC_TERM_ID==null?"null":SRC_INSTNL_NGTTD_SRVC_TERM_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_INSTNL_REIMBMNT_TERM_ID==null?"null":SRC_INSTNL_REIMBMNT_TERM_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_MEDCR_ID==null?"null":SRC_MEDCR_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_NST_SRVC_CTGRY_CD==null?"null":SRC_NST_SRVC_CTGRY_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PAY_ACTN_CD==null?"null":SRC_PAY_ACTN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PRCG_CRTRIA_CD==null?"null":SRC_PRCG_CRTRIA_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PRCG_RSN_CD==null?"null":SRC_PRCG_RSN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PROV_NATL_PROV_ID==null?"null":SRC_PROV_NATL_PROV_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PRTY_SCOR_NBR==null?"null":SRC_PRTY_SCOR_NBR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_SRVC_CLSFCTN_CD==null?"null":SRC_SRVC_CLSFCTN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_TERM_EFCTV_DT==null?"null":"" + SRC_TERM_EFCTV_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_TERM_END_DT==null?"null":"" + SRC_TERM_END_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TOS_CD==null?"null":TOS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(apcst==null?"null":apcst, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(apcwt==null?"null":"" + apcwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(betos==null?"null":betos, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(brand==null?"null":brand, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cat1==null?"null":cat1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cat2==null?"null":cat2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clmwt==null?"null":"" + clmwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(erwt==null?"null":"" + erwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fundlvl2==null?"null":fundlvl2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(liccd==null?"null":liccd, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(market==null?"null":market, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Inc_Month==null?"null":Inc_Month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(network==null?"null":network, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(normflag==null?"null":normflag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(priority==null?"null":priority, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(prodlvl3==null?"null":prodlvl3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(prov_county==null?"null":prov_county, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(surgrp==null?"null":surgrp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(surgwt==null?"null":"" + surgwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(system_id==null?"null":system_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(GRP_NM==null?"null":GRP_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_SUBGRP_NBR==null?"null":SRC_SUBGRP_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VISITS==null?"null":"" + VISITS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(GRP_SIC==null?"null":GRP_SIC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MCID==null?"null":MCID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ITS_SCCF_NBR==null?"null":CLM_ITS_SCCF_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBRSHP_SOR_CD==null?"null":MBRSHP_SOR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_RT_CTGRY_CD==null?"null":SRC_RT_CTGRY_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_RT_CTGRY_SUB_CD==null?"null":SRC_RT_CTGRY_SUB_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(run_date==null?"null":"" + run_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_LINE_SRVC_STRT_DT==null?"null":"" + CLM_LINE_SRVC_STRT_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VNDR_CD==null?"null":VNDR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REPRICE_CD==null?"null":REPRICE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VNDR_PROD_CD==null?"null":VNDR_PROD_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zip3==null?"null":zip3, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(ADJDCTN_DT==null?"null":"" + ADJDCTN_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADJDCTN_STTS==null?"null":ADJDCTN_STTS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADMSN_TYPE_CD==null?"null":ADMSN_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADMT_DT==null?"null":"" + ADMT_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ALWD_AMT==null?"null":"" + ALWD_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APC_MEDCR_ID==null?"null":APC_MEDCR_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APC_VRSN_NBR==null?"null":APC_VRSN_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BED_TYPE_CD==null?"null":BED_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLD_CHRG_AMT==null?"null":"" + BILLD_CHRG_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLD_SRVC_UNIT_CNT==null?"null":"" + BILLD_SRVC_UNIT_CNT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNFT_PAYMNT_STTS_CD==null?"null":BNFT_PAYMNT_STTS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CASES==null?"null":CASES.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ADJSTMNT_KEY==null?"null":CLM_ADJSTMNT_KEY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ADJSTMNT_NBR==null?"null":CLM_ADJSTMNT_NBR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_DISP_CD==null?"null":CLM_DISP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ITS_HOST_CD==null?"null":CLM_ITS_HOST_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_LINE_NBR==null?"null":CLM_LINE_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_LINE_REIMB_MTHD_CD==null?"null":CLM_LINE_REIMB_MTHD_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_NBR==null?"null":CLM_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_REIMBMNT_SOR_CD==null?"null":CLM_REIMBMNT_SOR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_REIMBMNT_TYPE_CD==null?"null":CLM_REIMBMNT_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_SOR_CD==null?"null":CLM_SOR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_STMNT_FROM_DT==null?"null":"" + CLM_STMNT_FROM_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_STMNT_TO_DT==null?"null":"" + CLM_STMNT_TO_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD==null?"null":"" + CMAD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD1==null?"null":"" + CMAD1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD_ALLOWED==null?"null":"" + CMAD_ALLOWED, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD_BILLED==null?"null":"" + CMAD_BILLED, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMAD_CASES==null?"null":CMAD_CASES.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CMPNY_CF_CD==null?"null":CMPNY_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CVRD_EXPNS_AMT==null?"null":"" + CVRD_EXPNS_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DERIVD_IND_CD==null?"null":DERIVD_IND_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DSCHRG_DT==null?"null":"" + DSCHRG_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DSCHRG_STTS_CD==null?"null":DSCHRG_STTS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ER_Flag==null?"null":ER_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PediatricFlag==null?"null":PediatricFlag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ENC_Flag==null?"null":ENC_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EXCHNG_CERTFN_CD==null?"null":EXCHNG_CERTFN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EXCHNG_IND_CD==null?"null":EXCHNG_IND_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EXCHNG_METAL_TYPE_CD==null?"null":EXCHNG_METAL_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FUNDG_CF_CD==null?"null":FUNDG_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(HLTH_SRVC_CD==null?"null":HLTH_SRVC_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROC_MDFR_1_CD==null?"null":PROC_MDFR_1_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROC_MDFR_2_CD==null?"null":PROC_MDFR_2_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(HMO_CLS_CD==null?"null":HMO_CLS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ICL_ALWD_AMT==null?"null":"" + ICL_ALWD_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ICL_PAID_AMT==null?"null":"" + ICL_PAID_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INN_CD==null?"null":INN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LEGACY==null?"null":LEGACY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LINE_ITEM_APC_WT_AMT==null?"null":"" + LINE_ITEM_APC_WT_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LINE_ITEM_PAYMNT_AMT==null?"null":"" + LINE_ITEM_PAYMNT_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_County==null?"null":MBR_County, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_KEY==null?"null":MBR_KEY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_State==null?"null":MBR_State, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_ZIP3==null?"null":MBR_ZIP3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBR_ZIP_CD==null?"null":MBR_ZIP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBU_CF_CD==null?"null":MBU_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl1==null?"null":MBUlvl1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl2==null?"null":MBUlvl2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl3==null?"null":MBUlvl3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBUlvl4==null?"null":MBUlvl4, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MCS==null?"null":MCS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MEDCR_ID==null?"null":MEDCR_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NRMLZD_AMT==null?"null":"" + NRMLZD_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NRMLZD_CASE==null?"null":NRMLZD_CASE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NTWK_ID==null?"null":NTWK_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_NTWK_KEY==null?"null":CLM_NTWK_KEY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OBS_Flag==null?"null":OBS_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUT_ITEM_STTS_IND_TXT==null?"null":OUT_ITEM_STTS_IND_TXT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PAID_AMT==null?"null":"" + PAID_AMT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PAID_SRVC_UNIT_CNT==null?"null":PAID_SRVC_UNIT_CNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PARG_STTS_CD==null?"null":PARG_STTS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PAYMNT_APC_NBR==null?"null":PAYMNT_APC_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PN_ID==null?"null":PN_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_CF_CD==null?"null":PROD_CF_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROD_ID==null?"null":PROD_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_CITY_NM==null?"null":PROV_CITY_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_ID==null?"null":PROV_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_NM==null?"null":PROV_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_ST_NM==null?"null":PROV_ST_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROV_ZIP_CD==null?"null":PROV_ZIP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RDCTN_CTGRY_CD==null?"null":RDCTN_CTGRY_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RELATIVE_WEIGHT==null?"null":"" + RELATIVE_WEIGHT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RELATIVE_WEIGHT1==null?"null":"" + RELATIVE_WEIGHT1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RNDRG_PROV_ID==null?"null":RNDRG_PROV_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RNDRG_PROV_ID_TYPE_CD==null?"null":RNDRG_PROV_ID_TYPE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RVNU_CD==null?"null":RVNU_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RVU_Flag==null?"null":RVU_Flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_APRVL_CD==null?"null":SRC_APRVL_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_BILLG_TAX_ID==null?"null":SRC_BILLG_TAX_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_CLM_DISP_CD==null?"null":SRC_CLM_DISP_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_FINDER_NBR==null?"null":SRC_FINDER_NBR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_GRP_NBR==null?"null":SRC_GRP_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_HMO_CLS_CD==null?"null":SRC_HMO_CLS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_INSTNL_NGTTD_SRVC_TERM_ID==null?"null":SRC_INSTNL_NGTTD_SRVC_TERM_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_INSTNL_REIMBMNT_TERM_ID==null?"null":SRC_INSTNL_REIMBMNT_TERM_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_MEDCR_ID==null?"null":SRC_MEDCR_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_NST_SRVC_CTGRY_CD==null?"null":SRC_NST_SRVC_CTGRY_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PAY_ACTN_CD==null?"null":SRC_PAY_ACTN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PRCG_CRTRIA_CD==null?"null":SRC_PRCG_CRTRIA_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PRCG_RSN_CD==null?"null":SRC_PRCG_RSN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PROV_NATL_PROV_ID==null?"null":SRC_PROV_NATL_PROV_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_PRTY_SCOR_NBR==null?"null":SRC_PRTY_SCOR_NBR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_SRVC_CLSFCTN_CD==null?"null":SRC_SRVC_CLSFCTN_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_TERM_EFCTV_DT==null?"null":"" + SRC_TERM_EFCTV_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_TERM_END_DT==null?"null":"" + SRC_TERM_END_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TOS_CD==null?"null":TOS_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(apcst==null?"null":apcst, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(apcwt==null?"null":"" + apcwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(betos==null?"null":betos, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(brand==null?"null":brand, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cat1==null?"null":cat1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(cat2==null?"null":cat2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(clmwt==null?"null":"" + clmwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(erwt==null?"null":"" + erwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(fundlvl2==null?"null":fundlvl2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(liccd==null?"null":liccd, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(market==null?"null":market, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Inc_Month==null?"null":Inc_Month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(network==null?"null":network, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(normflag==null?"null":normflag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(priority==null?"null":priority, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(prodlvl3==null?"null":prodlvl3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(prov_county==null?"null":prov_county, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(surgrp==null?"null":surgrp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(surgwt==null?"null":"" + surgwt, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(system_id==null?"null":system_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(GRP_NM==null?"null":GRP_NM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_SUBGRP_NBR==null?"null":SRC_SUBGRP_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VISITS==null?"null":"" + VISITS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(GRP_SIC==null?"null":GRP_SIC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MCID==null?"null":MCID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_ITS_SCCF_NBR==null?"null":CLM_ITS_SCCF_NBR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MBRSHP_SOR_CD==null?"null":MBRSHP_SOR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_RT_CTGRY_CD==null?"null":SRC_RT_CTGRY_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SRC_RT_CTGRY_SUB_CD==null?"null":SRC_RT_CTGRY_SUB_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(run_date==null?"null":"" + run_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CLM_LINE_SRVC_STRT_DT==null?"null":"" + CLM_LINE_SRVC_STRT_DT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VNDR_CD==null?"null":VNDR_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REPRICE_CD==null?"null":REPRICE_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VNDR_PROD_CD==null?"null":VNDR_PROD_CD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zip3==null?"null":zip3, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ADJDCTN_DT = null; } else {
      this.ADJDCTN_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ADJDCTN_STTS = null; } else {
      this.ADJDCTN_STTS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ADMSN_TYPE_CD = null; } else {
      this.ADMSN_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ADMT_DT = null; } else {
      this.ADMT_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ALWD_AMT = null; } else {
      this.ALWD_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.APC_MEDCR_ID = null; } else {
      this.APC_MEDCR_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.APC_VRSN_NBR = null; } else {
      this.APC_VRSN_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BED_TYPE_CD = null; } else {
      this.BED_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLD_CHRG_AMT = null; } else {
      this.BILLD_CHRG_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLD_SRVC_UNIT_CNT = null; } else {
      this.BILLD_SRVC_UNIT_CNT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNFT_PAYMNT_STTS_CD = null; } else {
      this.BNFT_PAYMNT_STTS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CASES = null; } else {
      this.CASES = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_ADJSTMNT_KEY = null; } else {
      this.CLM_ADJSTMNT_KEY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_ADJSTMNT_NBR = null; } else {
      this.CLM_ADJSTMNT_NBR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_DISP_CD = null; } else {
      this.CLM_DISP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_ITS_HOST_CD = null; } else {
      this.CLM_ITS_HOST_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_LINE_NBR = null; } else {
      this.CLM_LINE_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_LINE_REIMB_MTHD_CD = null; } else {
      this.CLM_LINE_REIMB_MTHD_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_NBR = null; } else {
      this.CLM_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_REIMBMNT_SOR_CD = null; } else {
      this.CLM_REIMBMNT_SOR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_REIMBMNT_TYPE_CD = null; } else {
      this.CLM_REIMBMNT_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_SOR_CD = null; } else {
      this.CLM_SOR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_STMNT_FROM_DT = null; } else {
      this.CLM_STMNT_FROM_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_STMNT_TO_DT = null; } else {
      this.CLM_STMNT_TO_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD = null; } else {
      this.CMAD = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD1 = null; } else {
      this.CMAD1 = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD_ALLOWED = null; } else {
      this.CMAD_ALLOWED = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD_BILLED = null; } else {
      this.CMAD_BILLED = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD_CASES = null; } else {
      this.CMAD_CASES = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CMPNY_CF_CD = null; } else {
      this.CMPNY_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CVRD_EXPNS_AMT = null; } else {
      this.CVRD_EXPNS_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DERIVD_IND_CD = null; } else {
      this.DERIVD_IND_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DSCHRG_DT = null; } else {
      this.DSCHRG_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DSCHRG_STTS_CD = null; } else {
      this.DSCHRG_STTS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ER_Flag = null; } else {
      this.ER_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PediatricFlag = null; } else {
      this.PediatricFlag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ENC_Flag = null; } else {
      this.ENC_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EXCHNG_CERTFN_CD = null; } else {
      this.EXCHNG_CERTFN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EXCHNG_IND_CD = null; } else {
      this.EXCHNG_IND_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EXCHNG_METAL_TYPE_CD = null; } else {
      this.EXCHNG_METAL_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FUNDG_CF_CD = null; } else {
      this.FUNDG_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.HLTH_SRVC_CD = null; } else {
      this.HLTH_SRVC_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROC_MDFR_1_CD = null; } else {
      this.PROC_MDFR_1_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROC_MDFR_2_CD = null; } else {
      this.PROC_MDFR_2_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.HMO_CLS_CD = null; } else {
      this.HMO_CLS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ICL_ALWD_AMT = null; } else {
      this.ICL_ALWD_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ICL_PAID_AMT = null; } else {
      this.ICL_PAID_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INN_CD = null; } else {
      this.INN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LEGACY = null; } else {
      this.LEGACY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.LINE_ITEM_APC_WT_AMT = null; } else {
      this.LINE_ITEM_APC_WT_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.LINE_ITEM_PAYMNT_AMT = null; } else {
      this.LINE_ITEM_PAYMNT_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_County = null; } else {
      this.MBR_County = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_KEY = null; } else {
      this.MBR_KEY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_State = null; } else {
      this.MBR_State = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_ZIP3 = null; } else {
      this.MBR_ZIP3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_ZIP_CD = null; } else {
      this.MBR_ZIP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBU_CF_CD = null; } else {
      this.MBU_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl1 = null; } else {
      this.MBUlvl1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl2 = null; } else {
      this.MBUlvl2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl3 = null; } else {
      this.MBUlvl3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl4 = null; } else {
      this.MBUlvl4 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MCS = null; } else {
      this.MCS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MEDCR_ID = null; } else {
      this.MEDCR_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NRMLZD_AMT = null; } else {
      this.NRMLZD_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NRMLZD_CASE = null; } else {
      this.NRMLZD_CASE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NTWK_ID = null; } else {
      this.NTWK_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_NTWK_KEY = null; } else {
      this.CLM_NTWK_KEY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OBS_Flag = null; } else {
      this.OBS_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUT_ITEM_STTS_IND_TXT = null; } else {
      this.OUT_ITEM_STTS_IND_TXT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PAID_AMT = null; } else {
      this.PAID_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PAID_SRVC_UNIT_CNT = null; } else {
      this.PAID_SRVC_UNIT_CNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PARG_STTS_CD = null; } else {
      this.PARG_STTS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PAYMNT_APC_NBR = null; } else {
      this.PAYMNT_APC_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PN_ID = null; } else {
      this.PN_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CF_CD = null; } else {
      this.PROD_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_ID = null; } else {
      this.PROD_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_CITY_NM = null; } else {
      this.PROV_CITY_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_ID = null; } else {
      this.PROV_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_NM = null; } else {
      this.PROV_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_ST_NM = null; } else {
      this.PROV_ST_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_ZIP_CD = null; } else {
      this.PROV_ZIP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RDCTN_CTGRY_CD = null; } else {
      this.RDCTN_CTGRY_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RELATIVE_WEIGHT = null; } else {
      this.RELATIVE_WEIGHT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RELATIVE_WEIGHT1 = null; } else {
      this.RELATIVE_WEIGHT1 = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RNDRG_PROV_ID = null; } else {
      this.RNDRG_PROV_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RNDRG_PROV_ID_TYPE_CD = null; } else {
      this.RNDRG_PROV_ID_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RVNU_CD = null; } else {
      this.RVNU_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RVU_Flag = null; } else {
      this.RVU_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_APRVL_CD = null; } else {
      this.SRC_APRVL_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_BILLG_TAX_ID = null; } else {
      this.SRC_BILLG_TAX_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_CLM_DISP_CD = null; } else {
      this.SRC_CLM_DISP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_FINDER_NBR = null; } else {
      this.SRC_FINDER_NBR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_GRP_NBR = null; } else {
      this.SRC_GRP_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_HMO_CLS_CD = null; } else {
      this.SRC_HMO_CLS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = null; } else {
      this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_INSTNL_REIMBMNT_TERM_ID = null; } else {
      this.SRC_INSTNL_REIMBMNT_TERM_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_MEDCR_ID = null; } else {
      this.SRC_MEDCR_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_NST_SRVC_CTGRY_CD = null; } else {
      this.SRC_NST_SRVC_CTGRY_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PAY_ACTN_CD = null; } else {
      this.SRC_PAY_ACTN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PRCG_CRTRIA_CD = null; } else {
      this.SRC_PRCG_CRTRIA_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PRCG_RSN_CD = null; } else {
      this.SRC_PRCG_RSN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PROV_NATL_PROV_ID = null; } else {
      this.SRC_PROV_NATL_PROV_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_PRTY_SCOR_NBR = null; } else {
      this.SRC_PRTY_SCOR_NBR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_SRVC_CLSFCTN_CD = null; } else {
      this.SRC_SRVC_CLSFCTN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_TERM_EFCTV_DT = null; } else {
      this.SRC_TERM_EFCTV_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_TERM_END_DT = null; } else {
      this.SRC_TERM_END_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TOS_CD = null; } else {
      this.TOS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.apcst = null; } else {
      this.apcst = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.apcwt = null; } else {
      this.apcwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.betos = null; } else {
      this.betos = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.brand = null; } else {
      this.brand = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cat1 = null; } else {
      this.cat1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cat2 = null; } else {
      this.cat2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.clmwt = null; } else {
      this.clmwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.erwt = null; } else {
      this.erwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.fundlvl2 = null; } else {
      this.fundlvl2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.liccd = null; } else {
      this.liccd = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.market = null; } else {
      this.market = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Inc_Month = null; } else {
      this.Inc_Month = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.network = null; } else {
      this.network = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.normflag = null; } else {
      this.normflag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.priority = null; } else {
      this.priority = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.prodlvl3 = null; } else {
      this.prodlvl3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.prov_county = null; } else {
      this.prov_county = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.surgrp = null; } else {
      this.surgrp = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.surgwt = null; } else {
      this.surgwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.system_id = null; } else {
      this.system_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRP_NM = null; } else {
      this.GRP_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_SUBGRP_NBR = null; } else {
      this.SRC_SUBGRP_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.VISITS = null; } else {
      this.VISITS = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRP_SIC = null; } else {
      this.GRP_SIC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MCID = null; } else {
      this.MCID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_ITS_SCCF_NBR = null; } else {
      this.CLM_ITS_SCCF_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBRSHP_SOR_CD = null; } else {
      this.MBRSHP_SOR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_RT_CTGRY_CD = null; } else {
      this.SRC_RT_CTGRY_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_RT_CTGRY_SUB_CD = null; } else {
      this.SRC_RT_CTGRY_SUB_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.run_date = null; } else {
      this.run_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_LINE_SRVC_STRT_DT = null; } else {
      this.CLM_LINE_SRVC_STRT_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.VNDR_CD = null; } else {
      this.VNDR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REPRICE_CD = null; } else {
      this.REPRICE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.VNDR_PROD_CD = null; } else {
      this.VNDR_PROD_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.zip3 = null; } else {
      this.zip3 = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ADJDCTN_DT = null; } else {
      this.ADJDCTN_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ADJDCTN_STTS = null; } else {
      this.ADJDCTN_STTS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ADMSN_TYPE_CD = null; } else {
      this.ADMSN_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ADMT_DT = null; } else {
      this.ADMT_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ALWD_AMT = null; } else {
      this.ALWD_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.APC_MEDCR_ID = null; } else {
      this.APC_MEDCR_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.APC_VRSN_NBR = null; } else {
      this.APC_VRSN_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BED_TYPE_CD = null; } else {
      this.BED_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLD_CHRG_AMT = null; } else {
      this.BILLD_CHRG_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLD_SRVC_UNIT_CNT = null; } else {
      this.BILLD_SRVC_UNIT_CNT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNFT_PAYMNT_STTS_CD = null; } else {
      this.BNFT_PAYMNT_STTS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CASES = null; } else {
      this.CASES = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_ADJSTMNT_KEY = null; } else {
      this.CLM_ADJSTMNT_KEY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_ADJSTMNT_NBR = null; } else {
      this.CLM_ADJSTMNT_NBR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_DISP_CD = null; } else {
      this.CLM_DISP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_ITS_HOST_CD = null; } else {
      this.CLM_ITS_HOST_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_LINE_NBR = null; } else {
      this.CLM_LINE_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_LINE_REIMB_MTHD_CD = null; } else {
      this.CLM_LINE_REIMB_MTHD_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_NBR = null; } else {
      this.CLM_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_REIMBMNT_SOR_CD = null; } else {
      this.CLM_REIMBMNT_SOR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_REIMBMNT_TYPE_CD = null; } else {
      this.CLM_REIMBMNT_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_SOR_CD = null; } else {
      this.CLM_SOR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_STMNT_FROM_DT = null; } else {
      this.CLM_STMNT_FROM_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_STMNT_TO_DT = null; } else {
      this.CLM_STMNT_TO_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD = null; } else {
      this.CMAD = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD1 = null; } else {
      this.CMAD1 = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD_ALLOWED = null; } else {
      this.CMAD_ALLOWED = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD_BILLED = null; } else {
      this.CMAD_BILLED = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CMAD_CASES = null; } else {
      this.CMAD_CASES = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CMPNY_CF_CD = null; } else {
      this.CMPNY_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CVRD_EXPNS_AMT = null; } else {
      this.CVRD_EXPNS_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DERIVD_IND_CD = null; } else {
      this.DERIVD_IND_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DSCHRG_DT = null; } else {
      this.DSCHRG_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DSCHRG_STTS_CD = null; } else {
      this.DSCHRG_STTS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ER_Flag = null; } else {
      this.ER_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PediatricFlag = null; } else {
      this.PediatricFlag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ENC_Flag = null; } else {
      this.ENC_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EXCHNG_CERTFN_CD = null; } else {
      this.EXCHNG_CERTFN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EXCHNG_IND_CD = null; } else {
      this.EXCHNG_IND_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EXCHNG_METAL_TYPE_CD = null; } else {
      this.EXCHNG_METAL_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FUNDG_CF_CD = null; } else {
      this.FUNDG_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.HLTH_SRVC_CD = null; } else {
      this.HLTH_SRVC_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROC_MDFR_1_CD = null; } else {
      this.PROC_MDFR_1_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROC_MDFR_2_CD = null; } else {
      this.PROC_MDFR_2_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.HMO_CLS_CD = null; } else {
      this.HMO_CLS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ICL_ALWD_AMT = null; } else {
      this.ICL_ALWD_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ICL_PAID_AMT = null; } else {
      this.ICL_PAID_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INN_CD = null; } else {
      this.INN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LEGACY = null; } else {
      this.LEGACY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.LINE_ITEM_APC_WT_AMT = null; } else {
      this.LINE_ITEM_APC_WT_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.LINE_ITEM_PAYMNT_AMT = null; } else {
      this.LINE_ITEM_PAYMNT_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_County = null; } else {
      this.MBR_County = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_KEY = null; } else {
      this.MBR_KEY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_State = null; } else {
      this.MBR_State = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_ZIP3 = null; } else {
      this.MBR_ZIP3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBR_ZIP_CD = null; } else {
      this.MBR_ZIP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBU_CF_CD = null; } else {
      this.MBU_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl1 = null; } else {
      this.MBUlvl1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl2 = null; } else {
      this.MBUlvl2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl3 = null; } else {
      this.MBUlvl3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBUlvl4 = null; } else {
      this.MBUlvl4 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MCS = null; } else {
      this.MCS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MEDCR_ID = null; } else {
      this.MEDCR_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NRMLZD_AMT = null; } else {
      this.NRMLZD_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NRMLZD_CASE = null; } else {
      this.NRMLZD_CASE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NTWK_ID = null; } else {
      this.NTWK_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_NTWK_KEY = null; } else {
      this.CLM_NTWK_KEY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OBS_Flag = null; } else {
      this.OBS_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUT_ITEM_STTS_IND_TXT = null; } else {
      this.OUT_ITEM_STTS_IND_TXT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PAID_AMT = null; } else {
      this.PAID_AMT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PAID_SRVC_UNIT_CNT = null; } else {
      this.PAID_SRVC_UNIT_CNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PARG_STTS_CD = null; } else {
      this.PARG_STTS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PAYMNT_APC_NBR = null; } else {
      this.PAYMNT_APC_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PN_ID = null; } else {
      this.PN_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_CF_CD = null; } else {
      this.PROD_CF_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROD_ID = null; } else {
      this.PROD_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_CITY_NM = null; } else {
      this.PROV_CITY_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_ID = null; } else {
      this.PROV_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_NM = null; } else {
      this.PROV_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_ST_NM = null; } else {
      this.PROV_ST_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROV_ZIP_CD = null; } else {
      this.PROV_ZIP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RDCTN_CTGRY_CD = null; } else {
      this.RDCTN_CTGRY_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RELATIVE_WEIGHT = null; } else {
      this.RELATIVE_WEIGHT = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RELATIVE_WEIGHT1 = null; } else {
      this.RELATIVE_WEIGHT1 = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RNDRG_PROV_ID = null; } else {
      this.RNDRG_PROV_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RNDRG_PROV_ID_TYPE_CD = null; } else {
      this.RNDRG_PROV_ID_TYPE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RVNU_CD = null; } else {
      this.RVNU_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RVU_Flag = null; } else {
      this.RVU_Flag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_APRVL_CD = null; } else {
      this.SRC_APRVL_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_BILLG_TAX_ID = null; } else {
      this.SRC_BILLG_TAX_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_CLM_DISP_CD = null; } else {
      this.SRC_CLM_DISP_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_FINDER_NBR = null; } else {
      this.SRC_FINDER_NBR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_GRP_NBR = null; } else {
      this.SRC_GRP_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_HMO_CLS_CD = null; } else {
      this.SRC_HMO_CLS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = null; } else {
      this.SRC_INSTNL_NGTTD_SRVC_TERM_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_INSTNL_REIMBMNT_TERM_ID = null; } else {
      this.SRC_INSTNL_REIMBMNT_TERM_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_MEDCR_ID = null; } else {
      this.SRC_MEDCR_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_NST_SRVC_CTGRY_CD = null; } else {
      this.SRC_NST_SRVC_CTGRY_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PAY_ACTN_CD = null; } else {
      this.SRC_PAY_ACTN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PRCG_CRTRIA_CD = null; } else {
      this.SRC_PRCG_CRTRIA_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PRCG_RSN_CD = null; } else {
      this.SRC_PRCG_RSN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_PROV_NATL_PROV_ID = null; } else {
      this.SRC_PROV_NATL_PROV_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_PRTY_SCOR_NBR = null; } else {
      this.SRC_PRTY_SCOR_NBR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_SRVC_CLSFCTN_CD = null; } else {
      this.SRC_SRVC_CLSFCTN_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_TERM_EFCTV_DT = null; } else {
      this.SRC_TERM_EFCTV_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.SRC_TERM_END_DT = null; } else {
      this.SRC_TERM_END_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TOS_CD = null; } else {
      this.TOS_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.apcst = null; } else {
      this.apcst = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.apcwt = null; } else {
      this.apcwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.betos = null; } else {
      this.betos = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.brand = null; } else {
      this.brand = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cat1 = null; } else {
      this.cat1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.cat2 = null; } else {
      this.cat2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.clmwt = null; } else {
      this.clmwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.erwt = null; } else {
      this.erwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.fundlvl2 = null; } else {
      this.fundlvl2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.liccd = null; } else {
      this.liccd = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.market = null; } else {
      this.market = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.Inc_Month = null; } else {
      this.Inc_Month = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.network = null; } else {
      this.network = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.normflag = null; } else {
      this.normflag = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.priority = null; } else {
      this.priority = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.prodlvl3 = null; } else {
      this.prodlvl3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.prov_county = null; } else {
      this.prov_county = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.surgrp = null; } else {
      this.surgrp = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.surgwt = null; } else {
      this.surgwt = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.system_id = null; } else {
      this.system_id = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRP_NM = null; } else {
      this.GRP_NM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_SUBGRP_NBR = null; } else {
      this.SRC_SUBGRP_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.VISITS = null; } else {
      this.VISITS = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRP_SIC = null; } else {
      this.GRP_SIC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MCID = null; } else {
      this.MCID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLM_ITS_SCCF_NBR = null; } else {
      this.CLM_ITS_SCCF_NBR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MBRSHP_SOR_CD = null; } else {
      this.MBRSHP_SOR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_RT_CTGRY_CD = null; } else {
      this.SRC_RT_CTGRY_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SRC_RT_CTGRY_SUB_CD = null; } else {
      this.SRC_RT_CTGRY_SUB_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.run_date = null; } else {
      this.run_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CLM_LINE_SRVC_STRT_DT = null; } else {
      this.CLM_LINE_SRVC_STRT_DT = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.VNDR_CD = null; } else {
      this.VNDR_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REPRICE_CD = null; } else {
      this.REPRICE_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.VNDR_PROD_CD = null; } else {
      this.VNDR_PROD_CD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.zip3 = null; } else {
      this.zip3 = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    o.ADJDCTN_DT = (o.ADJDCTN_DT != null) ? (java.sql.Date) o.ADJDCTN_DT.clone() : null;
    o.ADMT_DT = (o.ADMT_DT != null) ? (java.sql.Date) o.ADMT_DT.clone() : null;
    o.CLM_STMNT_FROM_DT = (o.CLM_STMNT_FROM_DT != null) ? (java.sql.Date) o.CLM_STMNT_FROM_DT.clone() : null;
    o.CLM_STMNT_TO_DT = (o.CLM_STMNT_TO_DT != null) ? (java.sql.Date) o.CLM_STMNT_TO_DT.clone() : null;
    o.DSCHRG_DT = (o.DSCHRG_DT != null) ? (java.sql.Date) o.DSCHRG_DT.clone() : null;
    o.SRC_TERM_EFCTV_DT = (o.SRC_TERM_EFCTV_DT != null) ? (java.sql.Date) o.SRC_TERM_EFCTV_DT.clone() : null;
    o.SRC_TERM_END_DT = (o.SRC_TERM_END_DT != null) ? (java.sql.Date) o.SRC_TERM_END_DT.clone() : null;
    o.run_date = (o.run_date != null) ? (java.sql.Date) o.run_date.clone() : null;
    o.CLM_LINE_SRVC_STRT_DT = (o.CLM_LINE_SRVC_STRT_DT != null) ? (java.sql.Date) o.CLM_LINE_SRVC_STRT_DT.clone() : null;
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
    o.ADJDCTN_DT = (o.ADJDCTN_DT != null) ? (java.sql.Date) o.ADJDCTN_DT.clone() : null;
    o.ADMT_DT = (o.ADMT_DT != null) ? (java.sql.Date) o.ADMT_DT.clone() : null;
    o.CLM_STMNT_FROM_DT = (o.CLM_STMNT_FROM_DT != null) ? (java.sql.Date) o.CLM_STMNT_FROM_DT.clone() : null;
    o.CLM_STMNT_TO_DT = (o.CLM_STMNT_TO_DT != null) ? (java.sql.Date) o.CLM_STMNT_TO_DT.clone() : null;
    o.DSCHRG_DT = (o.DSCHRG_DT != null) ? (java.sql.Date) o.DSCHRG_DT.clone() : null;
    o.SRC_TERM_EFCTV_DT = (o.SRC_TERM_EFCTV_DT != null) ? (java.sql.Date) o.SRC_TERM_EFCTV_DT.clone() : null;
    o.SRC_TERM_END_DT = (o.SRC_TERM_END_DT != null) ? (java.sql.Date) o.SRC_TERM_END_DT.clone() : null;
    o.run_date = (o.run_date != null) ? (java.sql.Date) o.run_date.clone() : null;
    o.CLM_LINE_SRVC_STRT_DT = (o.CLM_LINE_SRVC_STRT_DT != null) ? (java.sql.Date) o.CLM_LINE_SRVC_STRT_DT.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("ADJDCTN_DT", this.ADJDCTN_DT);
    __sqoop$field_map.put("ADJDCTN_STTS", this.ADJDCTN_STTS);
    __sqoop$field_map.put("ADMSN_TYPE_CD", this.ADMSN_TYPE_CD);
    __sqoop$field_map.put("ADMT_DT", this.ADMT_DT);
    __sqoop$field_map.put("ALWD_AMT", this.ALWD_AMT);
    __sqoop$field_map.put("APC_MEDCR_ID", this.APC_MEDCR_ID);
    __sqoop$field_map.put("APC_VRSN_NBR", this.APC_VRSN_NBR);
    __sqoop$field_map.put("BED_TYPE_CD", this.BED_TYPE_CD);
    __sqoop$field_map.put("BILLD_CHRG_AMT", this.BILLD_CHRG_AMT);
    __sqoop$field_map.put("BILLD_SRVC_UNIT_CNT", this.BILLD_SRVC_UNIT_CNT);
    __sqoop$field_map.put("BNFT_PAYMNT_STTS_CD", this.BNFT_PAYMNT_STTS_CD);
    __sqoop$field_map.put("CASES", this.CASES);
    __sqoop$field_map.put("CLM_ADJSTMNT_KEY", this.CLM_ADJSTMNT_KEY);
    __sqoop$field_map.put("CLM_ADJSTMNT_NBR", this.CLM_ADJSTMNT_NBR);
    __sqoop$field_map.put("CLM_DISP_CD", this.CLM_DISP_CD);
    __sqoop$field_map.put("CLM_ITS_HOST_CD", this.CLM_ITS_HOST_CD);
    __sqoop$field_map.put("CLM_LINE_NBR", this.CLM_LINE_NBR);
    __sqoop$field_map.put("CLM_LINE_REIMB_MTHD_CD", this.CLM_LINE_REIMB_MTHD_CD);
    __sqoop$field_map.put("CLM_NBR", this.CLM_NBR);
    __sqoop$field_map.put("CLM_REIMBMNT_SOR_CD", this.CLM_REIMBMNT_SOR_CD);
    __sqoop$field_map.put("CLM_REIMBMNT_TYPE_CD", this.CLM_REIMBMNT_TYPE_CD);
    __sqoop$field_map.put("CLM_SOR_CD", this.CLM_SOR_CD);
    __sqoop$field_map.put("CLM_STMNT_FROM_DT", this.CLM_STMNT_FROM_DT);
    __sqoop$field_map.put("CLM_STMNT_TO_DT", this.CLM_STMNT_TO_DT);
    __sqoop$field_map.put("CMAD", this.CMAD);
    __sqoop$field_map.put("CMAD1", this.CMAD1);
    __sqoop$field_map.put("CMAD_ALLOWED", this.CMAD_ALLOWED);
    __sqoop$field_map.put("CMAD_BILLED", this.CMAD_BILLED);
    __sqoop$field_map.put("CMAD_CASES", this.CMAD_CASES);
    __sqoop$field_map.put("CMPNY_CF_CD", this.CMPNY_CF_CD);
    __sqoop$field_map.put("CVRD_EXPNS_AMT", this.CVRD_EXPNS_AMT);
    __sqoop$field_map.put("DERIVD_IND_CD", this.DERIVD_IND_CD);
    __sqoop$field_map.put("DSCHRG_DT", this.DSCHRG_DT);
    __sqoop$field_map.put("DSCHRG_STTS_CD", this.DSCHRG_STTS_CD);
    __sqoop$field_map.put("ER_Flag", this.ER_Flag);
    __sqoop$field_map.put("PediatricFlag", this.PediatricFlag);
    __sqoop$field_map.put("ENC_Flag", this.ENC_Flag);
    __sqoop$field_map.put("EXCHNG_CERTFN_CD", this.EXCHNG_CERTFN_CD);
    __sqoop$field_map.put("EXCHNG_IND_CD", this.EXCHNG_IND_CD);
    __sqoop$field_map.put("EXCHNG_METAL_TYPE_CD", this.EXCHNG_METAL_TYPE_CD);
    __sqoop$field_map.put("FUNDG_CF_CD", this.FUNDG_CF_CD);
    __sqoop$field_map.put("HLTH_SRVC_CD", this.HLTH_SRVC_CD);
    __sqoop$field_map.put("PROC_MDFR_1_CD", this.PROC_MDFR_1_CD);
    __sqoop$field_map.put("PROC_MDFR_2_CD", this.PROC_MDFR_2_CD);
    __sqoop$field_map.put("HMO_CLS_CD", this.HMO_CLS_CD);
    __sqoop$field_map.put("ICL_ALWD_AMT", this.ICL_ALWD_AMT);
    __sqoop$field_map.put("ICL_PAID_AMT", this.ICL_PAID_AMT);
    __sqoop$field_map.put("INN_CD", this.INN_CD);
    __sqoop$field_map.put("LEGACY", this.LEGACY);
    __sqoop$field_map.put("LINE_ITEM_APC_WT_AMT", this.LINE_ITEM_APC_WT_AMT);
    __sqoop$field_map.put("LINE_ITEM_PAYMNT_AMT", this.LINE_ITEM_PAYMNT_AMT);
    __sqoop$field_map.put("MBR_County", this.MBR_County);
    __sqoop$field_map.put("MBR_KEY", this.MBR_KEY);
    __sqoop$field_map.put("MBR_State", this.MBR_State);
    __sqoop$field_map.put("MBR_ZIP3", this.MBR_ZIP3);
    __sqoop$field_map.put("MBR_ZIP_CD", this.MBR_ZIP_CD);
    __sqoop$field_map.put("MBU_CF_CD", this.MBU_CF_CD);
    __sqoop$field_map.put("MBUlvl1", this.MBUlvl1);
    __sqoop$field_map.put("MBUlvl2", this.MBUlvl2);
    __sqoop$field_map.put("MBUlvl3", this.MBUlvl3);
    __sqoop$field_map.put("MBUlvl4", this.MBUlvl4);
    __sqoop$field_map.put("MCS", this.MCS);
    __sqoop$field_map.put("MEDCR_ID", this.MEDCR_ID);
    __sqoop$field_map.put("NRMLZD_AMT", this.NRMLZD_AMT);
    __sqoop$field_map.put("NRMLZD_CASE", this.NRMLZD_CASE);
    __sqoop$field_map.put("NTWK_ID", this.NTWK_ID);
    __sqoop$field_map.put("CLM_NTWK_KEY", this.CLM_NTWK_KEY);
    __sqoop$field_map.put("OBS_Flag", this.OBS_Flag);
    __sqoop$field_map.put("OUT_ITEM_STTS_IND_TXT", this.OUT_ITEM_STTS_IND_TXT);
    __sqoop$field_map.put("PAID_AMT", this.PAID_AMT);
    __sqoop$field_map.put("PAID_SRVC_UNIT_CNT", this.PAID_SRVC_UNIT_CNT);
    __sqoop$field_map.put("PARG_STTS_CD", this.PARG_STTS_CD);
    __sqoop$field_map.put("PAYMNT_APC_NBR", this.PAYMNT_APC_NBR);
    __sqoop$field_map.put("PN_ID", this.PN_ID);
    __sqoop$field_map.put("PROD_CF_CD", this.PROD_CF_CD);
    __sqoop$field_map.put("PROD_ID", this.PROD_ID);
    __sqoop$field_map.put("PROV_CITY_NM", this.PROV_CITY_NM);
    __sqoop$field_map.put("PROV_ID", this.PROV_ID);
    __sqoop$field_map.put("PROV_NM", this.PROV_NM);
    __sqoop$field_map.put("PROV_ST_NM", this.PROV_ST_NM);
    __sqoop$field_map.put("PROV_ZIP_CD", this.PROV_ZIP_CD);
    __sqoop$field_map.put("RDCTN_CTGRY_CD", this.RDCTN_CTGRY_CD);
    __sqoop$field_map.put("RELATIVE_WEIGHT", this.RELATIVE_WEIGHT);
    __sqoop$field_map.put("RELATIVE_WEIGHT1", this.RELATIVE_WEIGHT1);
    __sqoop$field_map.put("RNDRG_PROV_ID", this.RNDRG_PROV_ID);
    __sqoop$field_map.put("RNDRG_PROV_ID_TYPE_CD", this.RNDRG_PROV_ID_TYPE_CD);
    __sqoop$field_map.put("RVNU_CD", this.RVNU_CD);
    __sqoop$field_map.put("RVU_Flag", this.RVU_Flag);
    __sqoop$field_map.put("SRC_APRVL_CD", this.SRC_APRVL_CD);
    __sqoop$field_map.put("SRC_BILLG_TAX_ID", this.SRC_BILLG_TAX_ID);
    __sqoop$field_map.put("SRC_CLM_DISP_CD", this.SRC_CLM_DISP_CD);
    __sqoop$field_map.put("SRC_FINDER_NBR", this.SRC_FINDER_NBR);
    __sqoop$field_map.put("SRC_GRP_NBR", this.SRC_GRP_NBR);
    __sqoop$field_map.put("SRC_HMO_CLS_CD", this.SRC_HMO_CLS_CD);
    __sqoop$field_map.put("SRC_INSTNL_NGTTD_SRVC_TERM_ID", this.SRC_INSTNL_NGTTD_SRVC_TERM_ID);
    __sqoop$field_map.put("SRC_INSTNL_REIMBMNT_TERM_ID", this.SRC_INSTNL_REIMBMNT_TERM_ID);
    __sqoop$field_map.put("SRC_MEDCR_ID", this.SRC_MEDCR_ID);
    __sqoop$field_map.put("SRC_NST_SRVC_CTGRY_CD", this.SRC_NST_SRVC_CTGRY_CD);
    __sqoop$field_map.put("SRC_PAY_ACTN_CD", this.SRC_PAY_ACTN_CD);
    __sqoop$field_map.put("SRC_PRCG_CRTRIA_CD", this.SRC_PRCG_CRTRIA_CD);
    __sqoop$field_map.put("SRC_PRCG_RSN_CD", this.SRC_PRCG_RSN_CD);
    __sqoop$field_map.put("SRC_PROV_NATL_PROV_ID", this.SRC_PROV_NATL_PROV_ID);
    __sqoop$field_map.put("SRC_PRTY_SCOR_NBR", this.SRC_PRTY_SCOR_NBR);
    __sqoop$field_map.put("SRC_SRVC_CLSFCTN_CD", this.SRC_SRVC_CLSFCTN_CD);
    __sqoop$field_map.put("SRC_TERM_EFCTV_DT", this.SRC_TERM_EFCTV_DT);
    __sqoop$field_map.put("SRC_TERM_END_DT", this.SRC_TERM_END_DT);
    __sqoop$field_map.put("TOS_CD", this.TOS_CD);
    __sqoop$field_map.put("apcst", this.apcst);
    __sqoop$field_map.put("apcwt", this.apcwt);
    __sqoop$field_map.put("betos", this.betos);
    __sqoop$field_map.put("brand", this.brand);
    __sqoop$field_map.put("cat1", this.cat1);
    __sqoop$field_map.put("cat2", this.cat2);
    __sqoop$field_map.put("clmwt", this.clmwt);
    __sqoop$field_map.put("erwt", this.erwt);
    __sqoop$field_map.put("fundlvl2", this.fundlvl2);
    __sqoop$field_map.put("liccd", this.liccd);
    __sqoop$field_map.put("market", this.market);
    __sqoop$field_map.put("Inc_Month", this.Inc_Month);
    __sqoop$field_map.put("network", this.network);
    __sqoop$field_map.put("normflag", this.normflag);
    __sqoop$field_map.put("priority", this.priority);
    __sqoop$field_map.put("prodlvl3", this.prodlvl3);
    __sqoop$field_map.put("prov_county", this.prov_county);
    __sqoop$field_map.put("surgrp", this.surgrp);
    __sqoop$field_map.put("surgwt", this.surgwt);
    __sqoop$field_map.put("system_id", this.system_id);
    __sqoop$field_map.put("GRP_NM", this.GRP_NM);
    __sqoop$field_map.put("SRC_SUBGRP_NBR", this.SRC_SUBGRP_NBR);
    __sqoop$field_map.put("VISITS", this.VISITS);
    __sqoop$field_map.put("GRP_SIC", this.GRP_SIC);
    __sqoop$field_map.put("MCID", this.MCID);
    __sqoop$field_map.put("CLM_ITS_SCCF_NBR", this.CLM_ITS_SCCF_NBR);
    __sqoop$field_map.put("MBRSHP_SOR_CD", this.MBRSHP_SOR_CD);
    __sqoop$field_map.put("SRC_RT_CTGRY_CD", this.SRC_RT_CTGRY_CD);
    __sqoop$field_map.put("SRC_RT_CTGRY_SUB_CD", this.SRC_RT_CTGRY_SUB_CD);
    __sqoop$field_map.put("run_date", this.run_date);
    __sqoop$field_map.put("CLM_LINE_SRVC_STRT_DT", this.CLM_LINE_SRVC_STRT_DT);
    __sqoop$field_map.put("VNDR_CD", this.VNDR_CD);
    __sqoop$field_map.put("REPRICE_CD", this.REPRICE_CD);
    __sqoop$field_map.put("VNDR_PROD_CD", this.VNDR_PROD_CD);
    __sqoop$field_map.put("zip3", this.zip3);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("ADJDCTN_DT", this.ADJDCTN_DT);
    __sqoop$field_map.put("ADJDCTN_STTS", this.ADJDCTN_STTS);
    __sqoop$field_map.put("ADMSN_TYPE_CD", this.ADMSN_TYPE_CD);
    __sqoop$field_map.put("ADMT_DT", this.ADMT_DT);
    __sqoop$field_map.put("ALWD_AMT", this.ALWD_AMT);
    __sqoop$field_map.put("APC_MEDCR_ID", this.APC_MEDCR_ID);
    __sqoop$field_map.put("APC_VRSN_NBR", this.APC_VRSN_NBR);
    __sqoop$field_map.put("BED_TYPE_CD", this.BED_TYPE_CD);
    __sqoop$field_map.put("BILLD_CHRG_AMT", this.BILLD_CHRG_AMT);
    __sqoop$field_map.put("BILLD_SRVC_UNIT_CNT", this.BILLD_SRVC_UNIT_CNT);
    __sqoop$field_map.put("BNFT_PAYMNT_STTS_CD", this.BNFT_PAYMNT_STTS_CD);
    __sqoop$field_map.put("CASES", this.CASES);
    __sqoop$field_map.put("CLM_ADJSTMNT_KEY", this.CLM_ADJSTMNT_KEY);
    __sqoop$field_map.put("CLM_ADJSTMNT_NBR", this.CLM_ADJSTMNT_NBR);
    __sqoop$field_map.put("CLM_DISP_CD", this.CLM_DISP_CD);
    __sqoop$field_map.put("CLM_ITS_HOST_CD", this.CLM_ITS_HOST_CD);
    __sqoop$field_map.put("CLM_LINE_NBR", this.CLM_LINE_NBR);
    __sqoop$field_map.put("CLM_LINE_REIMB_MTHD_CD", this.CLM_LINE_REIMB_MTHD_CD);
    __sqoop$field_map.put("CLM_NBR", this.CLM_NBR);
    __sqoop$field_map.put("CLM_REIMBMNT_SOR_CD", this.CLM_REIMBMNT_SOR_CD);
    __sqoop$field_map.put("CLM_REIMBMNT_TYPE_CD", this.CLM_REIMBMNT_TYPE_CD);
    __sqoop$field_map.put("CLM_SOR_CD", this.CLM_SOR_CD);
    __sqoop$field_map.put("CLM_STMNT_FROM_DT", this.CLM_STMNT_FROM_DT);
    __sqoop$field_map.put("CLM_STMNT_TO_DT", this.CLM_STMNT_TO_DT);
    __sqoop$field_map.put("CMAD", this.CMAD);
    __sqoop$field_map.put("CMAD1", this.CMAD1);
    __sqoop$field_map.put("CMAD_ALLOWED", this.CMAD_ALLOWED);
    __sqoop$field_map.put("CMAD_BILLED", this.CMAD_BILLED);
    __sqoop$field_map.put("CMAD_CASES", this.CMAD_CASES);
    __sqoop$field_map.put("CMPNY_CF_CD", this.CMPNY_CF_CD);
    __sqoop$field_map.put("CVRD_EXPNS_AMT", this.CVRD_EXPNS_AMT);
    __sqoop$field_map.put("DERIVD_IND_CD", this.DERIVD_IND_CD);
    __sqoop$field_map.put("DSCHRG_DT", this.DSCHRG_DT);
    __sqoop$field_map.put("DSCHRG_STTS_CD", this.DSCHRG_STTS_CD);
    __sqoop$field_map.put("ER_Flag", this.ER_Flag);
    __sqoop$field_map.put("PediatricFlag", this.PediatricFlag);
    __sqoop$field_map.put("ENC_Flag", this.ENC_Flag);
    __sqoop$field_map.put("EXCHNG_CERTFN_CD", this.EXCHNG_CERTFN_CD);
    __sqoop$field_map.put("EXCHNG_IND_CD", this.EXCHNG_IND_CD);
    __sqoop$field_map.put("EXCHNG_METAL_TYPE_CD", this.EXCHNG_METAL_TYPE_CD);
    __sqoop$field_map.put("FUNDG_CF_CD", this.FUNDG_CF_CD);
    __sqoop$field_map.put("HLTH_SRVC_CD", this.HLTH_SRVC_CD);
    __sqoop$field_map.put("PROC_MDFR_1_CD", this.PROC_MDFR_1_CD);
    __sqoop$field_map.put("PROC_MDFR_2_CD", this.PROC_MDFR_2_CD);
    __sqoop$field_map.put("HMO_CLS_CD", this.HMO_CLS_CD);
    __sqoop$field_map.put("ICL_ALWD_AMT", this.ICL_ALWD_AMT);
    __sqoop$field_map.put("ICL_PAID_AMT", this.ICL_PAID_AMT);
    __sqoop$field_map.put("INN_CD", this.INN_CD);
    __sqoop$field_map.put("LEGACY", this.LEGACY);
    __sqoop$field_map.put("LINE_ITEM_APC_WT_AMT", this.LINE_ITEM_APC_WT_AMT);
    __sqoop$field_map.put("LINE_ITEM_PAYMNT_AMT", this.LINE_ITEM_PAYMNT_AMT);
    __sqoop$field_map.put("MBR_County", this.MBR_County);
    __sqoop$field_map.put("MBR_KEY", this.MBR_KEY);
    __sqoop$field_map.put("MBR_State", this.MBR_State);
    __sqoop$field_map.put("MBR_ZIP3", this.MBR_ZIP3);
    __sqoop$field_map.put("MBR_ZIP_CD", this.MBR_ZIP_CD);
    __sqoop$field_map.put("MBU_CF_CD", this.MBU_CF_CD);
    __sqoop$field_map.put("MBUlvl1", this.MBUlvl1);
    __sqoop$field_map.put("MBUlvl2", this.MBUlvl2);
    __sqoop$field_map.put("MBUlvl3", this.MBUlvl3);
    __sqoop$field_map.put("MBUlvl4", this.MBUlvl4);
    __sqoop$field_map.put("MCS", this.MCS);
    __sqoop$field_map.put("MEDCR_ID", this.MEDCR_ID);
    __sqoop$field_map.put("NRMLZD_AMT", this.NRMLZD_AMT);
    __sqoop$field_map.put("NRMLZD_CASE", this.NRMLZD_CASE);
    __sqoop$field_map.put("NTWK_ID", this.NTWK_ID);
    __sqoop$field_map.put("CLM_NTWK_KEY", this.CLM_NTWK_KEY);
    __sqoop$field_map.put("OBS_Flag", this.OBS_Flag);
    __sqoop$field_map.put("OUT_ITEM_STTS_IND_TXT", this.OUT_ITEM_STTS_IND_TXT);
    __sqoop$field_map.put("PAID_AMT", this.PAID_AMT);
    __sqoop$field_map.put("PAID_SRVC_UNIT_CNT", this.PAID_SRVC_UNIT_CNT);
    __sqoop$field_map.put("PARG_STTS_CD", this.PARG_STTS_CD);
    __sqoop$field_map.put("PAYMNT_APC_NBR", this.PAYMNT_APC_NBR);
    __sqoop$field_map.put("PN_ID", this.PN_ID);
    __sqoop$field_map.put("PROD_CF_CD", this.PROD_CF_CD);
    __sqoop$field_map.put("PROD_ID", this.PROD_ID);
    __sqoop$field_map.put("PROV_CITY_NM", this.PROV_CITY_NM);
    __sqoop$field_map.put("PROV_ID", this.PROV_ID);
    __sqoop$field_map.put("PROV_NM", this.PROV_NM);
    __sqoop$field_map.put("PROV_ST_NM", this.PROV_ST_NM);
    __sqoop$field_map.put("PROV_ZIP_CD", this.PROV_ZIP_CD);
    __sqoop$field_map.put("RDCTN_CTGRY_CD", this.RDCTN_CTGRY_CD);
    __sqoop$field_map.put("RELATIVE_WEIGHT", this.RELATIVE_WEIGHT);
    __sqoop$field_map.put("RELATIVE_WEIGHT1", this.RELATIVE_WEIGHT1);
    __sqoop$field_map.put("RNDRG_PROV_ID", this.RNDRG_PROV_ID);
    __sqoop$field_map.put("RNDRG_PROV_ID_TYPE_CD", this.RNDRG_PROV_ID_TYPE_CD);
    __sqoop$field_map.put("RVNU_CD", this.RVNU_CD);
    __sqoop$field_map.put("RVU_Flag", this.RVU_Flag);
    __sqoop$field_map.put("SRC_APRVL_CD", this.SRC_APRVL_CD);
    __sqoop$field_map.put("SRC_BILLG_TAX_ID", this.SRC_BILLG_TAX_ID);
    __sqoop$field_map.put("SRC_CLM_DISP_CD", this.SRC_CLM_DISP_CD);
    __sqoop$field_map.put("SRC_FINDER_NBR", this.SRC_FINDER_NBR);
    __sqoop$field_map.put("SRC_GRP_NBR", this.SRC_GRP_NBR);
    __sqoop$field_map.put("SRC_HMO_CLS_CD", this.SRC_HMO_CLS_CD);
    __sqoop$field_map.put("SRC_INSTNL_NGTTD_SRVC_TERM_ID", this.SRC_INSTNL_NGTTD_SRVC_TERM_ID);
    __sqoop$field_map.put("SRC_INSTNL_REIMBMNT_TERM_ID", this.SRC_INSTNL_REIMBMNT_TERM_ID);
    __sqoop$field_map.put("SRC_MEDCR_ID", this.SRC_MEDCR_ID);
    __sqoop$field_map.put("SRC_NST_SRVC_CTGRY_CD", this.SRC_NST_SRVC_CTGRY_CD);
    __sqoop$field_map.put("SRC_PAY_ACTN_CD", this.SRC_PAY_ACTN_CD);
    __sqoop$field_map.put("SRC_PRCG_CRTRIA_CD", this.SRC_PRCG_CRTRIA_CD);
    __sqoop$field_map.put("SRC_PRCG_RSN_CD", this.SRC_PRCG_RSN_CD);
    __sqoop$field_map.put("SRC_PROV_NATL_PROV_ID", this.SRC_PROV_NATL_PROV_ID);
    __sqoop$field_map.put("SRC_PRTY_SCOR_NBR", this.SRC_PRTY_SCOR_NBR);
    __sqoop$field_map.put("SRC_SRVC_CLSFCTN_CD", this.SRC_SRVC_CLSFCTN_CD);
    __sqoop$field_map.put("SRC_TERM_EFCTV_DT", this.SRC_TERM_EFCTV_DT);
    __sqoop$field_map.put("SRC_TERM_END_DT", this.SRC_TERM_END_DT);
    __sqoop$field_map.put("TOS_CD", this.TOS_CD);
    __sqoop$field_map.put("apcst", this.apcst);
    __sqoop$field_map.put("apcwt", this.apcwt);
    __sqoop$field_map.put("betos", this.betos);
    __sqoop$field_map.put("brand", this.brand);
    __sqoop$field_map.put("cat1", this.cat1);
    __sqoop$field_map.put("cat2", this.cat2);
    __sqoop$field_map.put("clmwt", this.clmwt);
    __sqoop$field_map.put("erwt", this.erwt);
    __sqoop$field_map.put("fundlvl2", this.fundlvl2);
    __sqoop$field_map.put("liccd", this.liccd);
    __sqoop$field_map.put("market", this.market);
    __sqoop$field_map.put("Inc_Month", this.Inc_Month);
    __sqoop$field_map.put("network", this.network);
    __sqoop$field_map.put("normflag", this.normflag);
    __sqoop$field_map.put("priority", this.priority);
    __sqoop$field_map.put("prodlvl3", this.prodlvl3);
    __sqoop$field_map.put("prov_county", this.prov_county);
    __sqoop$field_map.put("surgrp", this.surgrp);
    __sqoop$field_map.put("surgwt", this.surgwt);
    __sqoop$field_map.put("system_id", this.system_id);
    __sqoop$field_map.put("GRP_NM", this.GRP_NM);
    __sqoop$field_map.put("SRC_SUBGRP_NBR", this.SRC_SUBGRP_NBR);
    __sqoop$field_map.put("VISITS", this.VISITS);
    __sqoop$field_map.put("GRP_SIC", this.GRP_SIC);
    __sqoop$field_map.put("MCID", this.MCID);
    __sqoop$field_map.put("CLM_ITS_SCCF_NBR", this.CLM_ITS_SCCF_NBR);
    __sqoop$field_map.put("MBRSHP_SOR_CD", this.MBRSHP_SOR_CD);
    __sqoop$field_map.put("SRC_RT_CTGRY_CD", this.SRC_RT_CTGRY_CD);
    __sqoop$field_map.put("SRC_RT_CTGRY_SUB_CD", this.SRC_RT_CTGRY_SUB_CD);
    __sqoop$field_map.put("run_date", this.run_date);
    __sqoop$field_map.put("CLM_LINE_SRVC_STRT_DT", this.CLM_LINE_SRVC_STRT_DT);
    __sqoop$field_map.put("VNDR_CD", this.VNDR_CD);
    __sqoop$field_map.put("REPRICE_CD", this.REPRICE_CD);
    __sqoop$field_map.put("VNDR_PROD_CD", this.VNDR_PROD_CD);
    __sqoop$field_map.put("zip3", this.zip3);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
