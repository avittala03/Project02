create 'dv_hb_pinnga1_csbd_r1a_wh:cogx_um', {NAME => 'cf1', COMPRESSION => 'SNAPPY'}, {NUMREGIONS => 16, SPLITALGO => 'HexStringSplit', DURABILITY => 'ASYNC_WAL'}