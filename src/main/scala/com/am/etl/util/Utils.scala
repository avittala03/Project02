/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */

package com.am.etl.util

import com.google.gson.GsonBuilder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectData
import org.apache.avro.reflect.ReflectDatumWriter
import java.io.ByteArrayOutputStream


/**
 * @author T Murali
 * @version 1.0
 *
 * Contains Helper functions t
 * 
 */

object Utils {

  /**
   * Serializes a POJO as an Avro instance
   */
  def serialize(aRecord: java.lang.Object): Array[Byte] = {
    val schema = ReflectData.AllowNull.get.getSchema(aRecord.getClass());
    val opStr = new ByteArrayOutputStream
    val encoder = new EncoderFactory().binaryEncoder(opStr, null)

    val writer = new ReflectDatumWriter[Object](schema)
    writer.write(aRecord, encoder)

    encoder.flush()
    opStr.close()
    opStr.toByteArray()

  }

  /**
   * Converts a POJO to a JSON Object
   */
  def asJSONString(aType: java.lang.Object): String = {
    val gson = new GsonBuilder().serializeNulls().create();
    gson.toJson(aType);
  }
   
}