package com.am.ndo.util

object NDOCommonUtils {

  def argsErrorMsg(): String = {
    """Rendering Spend Driver program needs exactly 3 arguments.
       | 1. Configuration file path
       | 2. Environment
       | 3. Query File Category""".stripMargin
  }
  def args4ErrorMsg(): String = {
    """Rendering Spend Driver program needs exactly 4 arguments.
    		   | 1. Configuration file path
    		   | 2. Environment
    		   | 3. Query File Category
    		   | 4. Xwalk type or PaymentType""".stripMargin
  }
}