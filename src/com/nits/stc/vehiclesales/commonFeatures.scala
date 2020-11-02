
package com.nits.stc.vehiclesales

import com.nits.etlcore.impl._
import org.apache.spark.sql._
import com.nits.util._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag


import org.apache.spark.sql.expressions.Window
import com.nits.global._
import com.nits.etlcore.impl._
import com.nits.vehiclesales.VehicleSalesTransformer
import com.nits.util.DataOperations

import com.nits.util.Config
import com.nits.util.MessageLogger
import com.nits.brms._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType

class commonFeatures {
   
  
   var uri ="hdfs://saptadwipa-Aspire-A515-54G:8020"
   var path="/config/vehiclesales/vehicleSalesConfig.json"
   var config = Config.readConfig(uri + path).params.asInstanceOf[Map[String,String]]
   var options= Config.readNestedKey(config,"SOURCE")
  
   var sourceType:Resource.ResourceType = Resource.withName(options("TYPE"))
   //var sourcedel:Resource.ResourceType = Resource.withName(options("DELIMITER"))
   var sourcePath = uri + options("PATH") 
   var Extractor=new DFExtractor()
   var sourceDF = Extractor.extract(Resource.TEXT,uri +"//raw/vw/vehiclesales",options)
    var statusOptions = Config.readNestedKey(config, "STATUS")
      var statusPath = uri + statusOptions("PATH")
      var statusCols = statusOptions("COLUMNS").split(",").toSeq
      var statusColumn = statusOptions("STATUS_COLUMN")
      var denormAttributes = Config.readNestedKey(config, "DENORMALIZE")

      for (i <- 1 to denormAttributes("TOTAL").toInt)
        denormAttributes = denormAttributes + ("ENTITY_" + i -> (uri + denormAttributes("ENTITY_" + i)))
   def getVehicleFeatures(sourceDF:DataFrame):DataFrame = 
   {
        
         if (sourceDF == null) return null
           println(sourceDF.count)
         var finalDF = sourceDF   
         println(finalDF.count)
         finalDF = finalDF.withColumn("ID",monotonically_increasing_id())
         println(finalDF.count)
         finalDF = finalDF.filter(col("SVF_FEATURE_DESC").isNotNull)
         println(finalDF.count)
         var windowPartition = Window.partitionBy(col("VIN")).orderBy(col("ID").desc)
         println(finalDF.count)
         finalDF = finalDF.withColumn("ROWNUMBER",row_number().over(windowPartition))
         println(finalDF.count)
         finalDF = finalDF.filter(col("ROWNUMBER") === lit(1))
           println(finalDF.count)
         finalDF = finalDF.withColumn("SVF_FEATURE_DESC",explode(split(col("SVF_FEATURE_DESC"),"\\|")))
           println(finalDF.count)
         finalDF = finalDF.drop("ID","ROWNUMBER")
        print( finalDF.count)
          var featuresDefintion = new DFDefinition()
      featuresDefintion.setSchema(uri + Config.readNestedValue(config, "SCHEMA", "VEHICLEFEATURES"), Resource.JSON)
print("success")
      var featuresTransformer = new DFTransformer(featuresDefintion)
        finalDF= featuresTransformer.applySchema(finalDF)
      finalDF=  featuresTransformer.applyTargetColumns(finalDF)
      print(finalDF.count)
        
        
        //finalDF.show()
         finalDF
              
    }
   def getSalesTransactions(sourceDF:DataFrame):DataFrame = {
        if (sourceDF == null) return null
        var finalDF = sourceDF  
        
        finalDF = finalDF.withColumn("ID",monotonically_increasing_id())

        var windowPartition = Window.partitionBy(col("DLR_CODE"),col("DEAL_NUMBER"),col("VIN"))
                                    .orderBy(col("ID").desc)
        
        finalDF = finalDF.withColumn("ROWNUMBER", row_number().over(windowPartition))
        finalDF = finalDF.filter(col("ROWNUMBER") === lit(1))
        
        finalDF = finalDF.withColumn("TRADE_VEHICLES",explode_outer(split(col("TRADE_VEHICLES"),"\\|")))
        finalDF = finalDF.dropDuplicates()
        
        finalDF = finalDF.withColumn("TRADE_VEHICLES",split(col("TRADE_VEHICLES"),"\\~"))
        
        finalDF = finalDF.withColumn("STV_VIN",col("TRADE_VEHICLES")(0))
        finalDF = finalDF.withColumn("STV_TYPE_CODE",col("TRADE_VEHICLES")(1))
        finalDF = finalDF.withColumn("STV_MAKE",col("TRADE_VEHICLES")(2))
        finalDF = finalDF.withColumn("STV_MODEL",col("TRADE_VEHICLES")(3))
        finalDF = finalDF.withColumn("STV_MODEL_YEAR",col("TRADE_VEHICLES")(4))
        finalDF = finalDF.withColumn("STV_TRIM_LVL",col("TRADE_VEHICLES")(5))
        finalDF = finalDF.withColumn("STV_SUBTRIM_LVL",col("TRADE_VEHICLES")(6))
        finalDF = finalDF.withColumn("STV_EXT_COLOR_DESC",col("TRADE_VEHICLES")(7))
        finalDF = finalDF.withColumn("STV_BODY_DESC",col("TRADE_VEHICLES")(8))
        finalDF = finalDF.withColumn("STV_DOOR_CNT",col("TRADE_VEHICLES")(9))
        finalDF = finalDF.withColumn("STV_TRANSMISSION_DESC",col("TRADE_VEHICLES")(10))
        finalDF = finalDF.withColumn("STV_ENGINE_DESC",col("TRADE_VEHICLES")(11))
        finalDF = finalDF.withColumn("STV_MODEL_CODE",col("TRADE_VEHICLES")(12))
        finalDF = finalDF.withColumn("STV_ALLOWANCE_AMT",col("TRADE_VEHICLES")(13))
        finalDF = finalDF.withColumn("STV_CASH_VALUE",col("TRADE_VEHICLES")(14))
        finalDF = finalDF.withColumn("STV_PAYOFF_AMT",col("TRADE_VEHICLES")(15))
        finalDF = finalDF.withColumn("STV_NET_AMT",col("TRADE_VEHICLES")(16))
        
        finalDF = finalDF.drop("ID","ROWNUMBER","TRADE_VEHICLES")
        
        var employeeDF = finalDF
        employeeDF = employeeDF.withColumn("SALES_PEOPLE",explode_outer(split(col("SALES_PEOPLE"),"\\|")))
        employeeDF = employeeDF.withColumn("SALES_PEOPLE",split(col("SALES_PEOPLE"),"\\~"))
        
        employeeDF = employeeDF.withColumn("EMP_ID",col("SALES_PEOPLE")(0))
       employeeDF = employeeDF.withColumn("EMP_TYPE",
                                        when(col("EMP_ID").isNotNull && col("EMP_ID") =!= lit("") ,lit("P"))
                                        .otherwise(lit(null)))
                                        
        employeeDF = employeeDF.withColumn("EMP_NAME",col("SALES_PEOPLE")(6))
        employeeDF = employeeDF.drop("SALES_PEOPLE")
        
        var salesManagerDF = employeeDF
        var fiManagerDF = employeeDF
        
        //salesManagerDF = salesManagerDF.filter(col("SALES_MGR_ID").isNotNull && col("SALES_MGR_ID") =!= lit("))
        salesManagerDF = salesManagerDF.filter(length(trim(col("SALES_MGR_ID"))) !== lit(0))
        salesManagerDF = salesManagerDF.withColumn("EMP_ID",col("SALES_MGR_ID"))
        salesManagerDF = salesManagerDF.withColumn("EMP_NAME",col("SALES_MGR_FULL_NAME"))
        salesManagerDF = salesManagerDF.withColumn("EMP_TYPE",lit("M"))
        
        //fiManagerDF = fiManagerDF.filter(col("FI_MGR_ID").isNotNull && col("FI_MGR_ID") =!= lit("))
        fiManagerDF = fiManagerDF.filter(length(trim(col("FI_MGR_ID"))) !== lit(0))
        fiManagerDF = fiManagerDF.withColumn("EMP_ID",col("FI_MGR_ID"))
        fiManagerDF = fiManagerDF.withColumn("EMP_NAME",col("FI_MGR_FULL_NAME"))
        fiManagerDF = fiManagerDF.withColumn("EMP_TYPE",lit("F"))
        
        finalDF = employeeDF union salesManagerDF union fiManagerDF
        finalDF = finalDF.dropDuplicates()
           var STRANSACTIONDefintion = new DFDefinition()
      STRANSACTIONDefintion.setSchema(uri + Config.readNestedValue(config, "SCHEMA", "SALESTRANSACTIONS"), Resource.JSON)
print("success")
      var STransactionTransformer = new DFTransformer(STRANSACTIONDefintion)
        finalDF= STransactionTransformer.applySchema(finalDF)
     finalDF=  STransactionTransformer.applyTargetColumns(finalDF)
     finalDF.printSchema()
      //  finalDF.show()
         finalDF
    	
        
    }
  def getSalesEmployees(sourceDF:DataFrame):DataFrame = {
        if (sourceDF == null) return null
        var employeeDF = sourceDF     
        var transactionPartition = Window.partitionBy(col("DLR_CODE"),col("DEAL_NUMBER"),col("VIN"))
			                                .orderBy(col("ID").desc)
    	  var employeePartition = Window.partitionBy(col("DLR_CODE"),col("SDE_EMP_ID"))
    			                      .orderBy(col("ID").desc)
    			                      
        employeeDF = sourceDF.withColumn("ID", monotonically_increasing_id())                               
    		employeeDF = employeeDF.withColumn("ROWNUMBER", row_number().over(transactionPartition))
    		employeeDF = employeeDF.filter(col("ROWNUMBER") === lit(1))
    	  employeeDF = employeeDF.drop("ROWNUMBER","ID")

    	  var salesManagerDF = employeeDF.drop("SALES_PEOPLE")
    	  var fiManagerDF = employeeDF.drop("SALES_PEOPLE")
    	      	      
    		employeeDF = employeeDF.withColumn("SALES_PEOPLE",explode(split(col("SALES_PEOPLE"),"\\|")))
    		employeeDF = employeeDF.withColumn("SALES_PEOPLE",split(col("SALES_PEOPLE"),"\\~"))
    		employeeDF = employeeDF.withColumn("SDE_EMP_ID",col("SALES_PEOPLE")(0))
    		employeeDF = employeeDF.withColumn("SDE_SALUTATION",col("SALES_PEOPLE")(1))
    		employeeDF = employeeDF.withColumn("SDE_FIRST_NAME",col("SALES_PEOPLE")(2))
    		employeeDF = employeeDF.withColumn("SDE_MIDDLE_NAME",col("SALES_PEOPLE")(3))
    		employeeDF = employeeDF.withColumn("SDE_LAST_NAME",col("SALES_PEOPLE")(4))
    		employeeDF = employeeDF.withColumn("SDE_SUFFIX",col("SALES_PEOPLE")(5))
    		employeeDF = employeeDF.withColumn("SDE_FULL_NAME",col("SALES_PEOPLE")(6))    		
    		employeeDF = employeeDF.withColumn("SDE_TYPE",lit("P"))
    		employeeDF = employeeDF.filter((col("SDE_EMP_ID").isNotNull) && (length(trim(col("SDE_EMP_ID"))) =!= lit(0)))
    		employeeDF = employeeDF.drop("SALES_PEOPLE")
    	  
    	  salesManagerDF = salesManagerDF.withColumn("SDE_EMP_ID", col("SALES_MGR_ID"))
    	  salesManagerDF = salesManagerDF.withColumn("SDE_SALUTATION", col("SALES_MGR_SALUTATION"))
    	  salesManagerDF = salesManagerDF.withColumn("SDE_FIRST_NAME", col("SALES_MGR_FIRST_NAME"))
    	  salesManagerDF = salesManagerDF.withColumn("SDE_MIDDLE_NAME", col("SALES_MGR_MIDDLE_NAME"))
    	  salesManagerDF = salesManagerDF.withColumn("SDE_LAST_NAME", col("SALES_MGR_LAST_NAME"))
    	  salesManagerDF = salesManagerDF.withColumn("SDE_SUFFIX", col("SALES_MGR_SUFFIX"))
    	  salesManagerDF = salesManagerDF.withColumn("SDE_FULL_NAME", col("SALES_MGR_FULL_NAME"))    	  
    	  salesManagerDF = salesManagerDF.filter((col("SDE_EMP_ID").isNotNull) && (length(trim(col("SDE_EMP_ID"))) =!= lit(0)))
    		salesManagerDF = salesManagerDF.withColumn("SDE_TYPE",lit("M"))    
    	  
    	  fiManagerDF = fiManagerDF.withColumn("SDE_EMP_ID", col("FI_MGR_ID"))
    	  fiManagerDF = fiManagerDF.withColumn("SDE_SALUTATION", col("FI_MGR_SALUTATION"))
    	  fiManagerDF = fiManagerDF.withColumn("SDE_FIRST_NAME", col("FI_MGR_FIRST_NAME"))
    	  fiManagerDF = fiManagerDF.withColumn("SDE_MIDDLE_NAME", col("FI_MGR_MIDDLE_NAME"))
    	  fiManagerDF = fiManagerDF.withColumn("SDE_LAST_NAME", col("FI_MGR_LAST_NAME"))
    	  fiManagerDF = fiManagerDF.withColumn("SDE_SUFFIX", col("FI_MGR_SUFFIX"))
    	  fiManagerDF = fiManagerDF.withColumn("SDE_FULL_NAME", col("FI_MGR_FULL_NAME"))
    	  fiManagerDF = fiManagerDF.filter((col("SDE_EMP_ID").isNotNull) && (length(trim(col("SDE_EMP_ID"))) =!= lit(0)))
    	  fiManagerDF = fiManagerDF.withColumn("SDE_TYPE",lit("F"))
    	  
        employeeDF = employeeDF union salesManagerDF union fiManagerDF
        
        employeeDF = employeeDF.dropDuplicates()
        
        employeeDF = employeeDF.withColumn("ID", monotonically_increasing_id())        
        employeeDF = employeeDF.withColumn("ROWNUMBER",row_number().over(employeePartition))
    		employeeDF = employeeDF.filter(col("ROWNUMBER") === lit(1))    
    	  employeeDF = employeeDF.drop("ID","ROWNUMBER")
    	  
          var STDefintion = new DFDefinition()
      STDefintion.setSchema(uri + Config.readNestedValue(config, "SCHEMA", "SALESEMPLOYEES"), Resource.JSON)
print("success")
      var STTransformer = new DFTransformer(STDefintion)
        employeeDF= STTransformer.applySchema(employeeDF)
     employeeDF=  STTransformer.applyTargetColumns(employeeDF)
       // employeeDF.show()
         employeeDF
    }   
    	 
    	
        
    
    def renameColumns(inputDF:DataFrame):DataFrame ={
      
    var sourceDF=inputDF
    sourceDF =  sourceDF.withColumnRenamed("_c0", "DLR_CODE")
    sourceDF =  sourceDF.withColumnRenamed("_c1", "CONTRACT_TYPE")
    sourceDF =  sourceDF.withColumnRenamed("_c2", "SALE_TYPE")
    sourceDF =  sourceDF.withColumnRenamed("_c3", "PURCHASE_ORDER_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c4", "CONTRACT_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c5", "DELIVERY_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c6", "REVERSAL_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c7", "INVENTORY_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c6", "REVERSAL_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c7", "INVENTORY_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c8", "DEAL_NUMBER")
    sourceDF =  sourceDF.withColumnRenamed("_c9", "DEAL_STATUS")
    sourceDF =  sourceDF.withColumnRenamed("_c10", "DEAL_STATUS_DATE")
    sourceDF =  sourceDF.withColumnRenamed("_c11", "VIN")
    sourceDF =  sourceDF.withColumnRenamed("_c12", "TYPE_CODE")
    sourceDF =  sourceDF.withColumnRenamed("_c13", "MAKE")
    sourceDF =  sourceDF.withColumnRenamed("_c14", "MODEL")
    sourceDF =  sourceDF.withColumnRenamed("_c15", "MODEL_YEAR")
    sourceDF =  sourceDF.withColumnRenamed("_c16", "TRIM_LEVEL")
    sourceDF =  sourceDF.withColumnRenamed("_c17", "SUB_TRIM_LEVEL")
    sourceDF =  sourceDF.withColumnRenamed("_c18", "EXT_COLOR_DESC")
    sourceDF =  sourceDF.withColumnRenamed("_c19", "BODY_DESC")
    sourceDF =  sourceDF.withColumnRenamed("_c20", "BODY_DOOR_CNT")
    sourceDF =  sourceDF.withColumnRenamed("_c21", "TRANSMISSION_DESC")
    sourceDF =  sourceDF.withColumnRenamed("_c22", "ENGINE_DESC")
    sourceDF =  sourceDF.withColumnRenamed("_c23", "SVF_FEATURE_DESC")
    sourceDF =  sourceDF.withColumnRenamed("_c24", "MODEL_CODE")
     


   sourceDF =  sourceDF.withColumnRenamed("_c25", "LICENSE_PLT_NUM")
   sourceDF =  sourceDF.withColumnRenamed("_c26", "REGSTRATION_AUTH")
   sourceDF =  sourceDF.withColumnRenamed("_c27", "INVENTORY_TYPE")
   sourceDF =  sourceDF.withColumnRenamed("_c28", "INVOICE_PRICE")
   sourceDF =  sourceDF.withColumnRenamed("_c29", "HOLDBACK_AMOUNT")
   sourceDF =  sourceDF.withColumnRenamed("_c30", "PACK_AMOUNT")
   sourceDF =  sourceDF.withColumnRenamed("_c31", "COST")
   sourceDF =  sourceDF.withColumnRenamed("_c32","RECONDITIONING_COST")
   sourceDF =  sourceDF.withColumnRenamed("_c33","LIST_PRICE")
   sourceDF =  sourceDF.withColumnRenamed("_c34","MFG_SUG_RETAIL_PRICE")

sourceDF =  sourceDF.withColumnRenamed("_c35", "STOCK_NUMBER")
sourceDF =  sourceDF.withColumnRenamed("_c36", "IS_CERTIFIED_FLAG")
sourceDF =  sourceDF.withColumnRenamed("_c37", "VEHICLE_SALE_PRICE")
sourceDF =  sourceDF.withColumnRenamed("_c38", "TOTAL_SALE_CREDIT_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c39", "TOTAL_PICKUP_PAYMENT")
sourceDF =  sourceDF.withColumnRenamed("_c40", "TOTAL_CASH_DOWN_PAYMENT")
sourceDF =  sourceDF.withColumnRenamed("_c41", "TOTAL_REBATE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c42", "TOTAL_TAXES")
sourceDF =  sourceDF.withColumnRenamed("_c43", "TOTAL_ACCESSORIES")
sourceDF =  sourceDF.withColumnRenamed("_c44", "TOTAL_FEES_AND_ACCESSORIES")
sourceDF =  sourceDF.withColumnRenamed("_c45", "TOTAL_TRADES_ALLOWANCE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c46", "TOTAL_TRAD_ACTUAL_CASH_VAL")
sourceDF =  sourceDF.withColumnRenamed("_c47", "TOTAL_TRAD_PAYOFF")
sourceDF =  sourceDF.withColumnRenamed("_c48", "TOTAL_NET_TRADE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c49", "TOTAL_GROSS_PROFIT")
sourceDF =  sourceDF.withColumnRenamed("_c50", "BACKEND_GROSS_PROFIT")
sourceDF =  sourceDF.withColumnRenamed("_c51", "FRONTEND_GROSS_PROFIT")
sourceDF =  sourceDF.withColumnRenamed("_c52", "SECURITY_DEPOSIT")
sourceDF =  sourceDF.withColumnRenamed("_c53", "TOTAL_DRIVEOFF_AMOUNT")
sourceDF =  sourceDF.withColumnRenamed("_c54", "NET_CAPITALIZED_COST")
sourceDF =  sourceDF.withColumnRenamed("_c55", "COMMENTS")
sourceDF =  sourceDF.withColumnRenamed("_c56", "DELIVERY_ODOMETER")
sourceDF =  sourceDF.withColumnRenamed("_c57", "TOTAL_FINANCE_AMOUNT")
sourceDF =  sourceDF.withColumnRenamed("_c58", "FINANCE_APR")
sourceDF =  sourceDF.withColumnRenamed("_c59", "FINANCE_CHARGE")
sourceDF =  sourceDF.withColumnRenamed("_c60", "CONTRACT_TERM")
sourceDF =  sourceDF.withColumnRenamed("_c61", "MONTHLY_PAYMENT")
sourceDF =  sourceDF.withColumnRenamed("_c62", "TOTAL_PAYMENTS")
sourceDF =  sourceDF.withColumnRenamed("_c63", "PAYMENT_FREQUENCY")
sourceDF =  sourceDF.withColumnRenamed("_c64", "FIRST_PAYMENT_DATE")
sourceDF =  sourceDF.withColumnRenamed("_c65", "EXPECTED_VEHIC_PAYOFF_DATE")
sourceDF =  sourceDF.withColumnRenamed("_c66","BALLOON_PAYMENT")
sourceDF =  sourceDF.withColumnRenamed("_c67", "FINANCE_COMPANY_CODE")
sourceDF =  sourceDF.withColumnRenamed("_c68", "FINANCE_COMPANY_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c69", "BUY_RATE")
sourceDF =  sourceDF.withColumnRenamed("_c70","BASE_RENTAL_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c71" ,"TERM_DEPRECIATION_VALUE")
sourceDF =  sourceDF.withColumnRenamed("_c72", "TOTAL_ESTIMATED_MILES")
sourceDF =  sourceDF.withColumnRenamed("_c73", "TOTAL_MILEAGE_LIMIT")
sourceDF =  sourceDF.withColumnRenamed("_c74", "RESIDUAL_AMOUNT")
sourceDF =  sourceDF.withColumnRenamed("_c75", "BUYER_ID")
sourceDF =  sourceDF.withColumnRenamed("_c76", "BUYER_SALUTATION")
sourceDF =  sourceDF.withColumnRenamed("_c77", "BUYER_FIRST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c78", "BUYER_MIDDLE_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c79", "BUYER_LAST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c80", "BUYER_SUFFIX")
sourceDF =  sourceDF.withColumnRenamed("_c81", "BUYER_FULL_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c82", "BUYER_BIRTH_DATE")
sourceDF =  sourceDF.withColumnRenamed("_c83", "BUYER_HOME_ADDR")
sourceDF =  sourceDF.withColumnRenamed("_c84", "BUYER_ADDR_DISTRICT")
sourceDF =  sourceDF.withColumnRenamed("_c85", "BUYER_ADDR_CITY")
sourceDF =  sourceDF.withColumnRenamed("_c86", "BUYER_ADDR_REGION")
sourceDF =  sourceDF.withColumnRenamed("_c87", "BUYER_ADDR_POSTAL_CODE")
sourceDF =  sourceDF.withColumnRenamed("_c88", "BUYER_HOME_COUNTRY")
sourceDF =  sourceDF.withColumnRenamed("_c89", "BUYER_HOME_PHONE_NUM")
sourceDF =  sourceDF.withColumnRenamed("_c90", "BUYER_HOME_EXTENSION")
sourceDF =  sourceDF.withColumnRenamed("_c91", "BUYER_HOME_PH_CNTRY_CODE")
sourceDF =  sourceDF.withColumnRenamed("_c92", "BUYER_BUS_PHONE_NUM")
sourceDF =  sourceDF.withColumnRenamed("_c93", "BUYER_BUS_EXTENSION")
sourceDF =  sourceDF.withColumnRenamed("_c94", "BUYER_BUS_PH_CNTRY_CODE")
sourceDF =  sourceDF.withColumnRenamed("_c95", "BUYER_EMAIL_ADDRESS")
sourceDF =  sourceDF.withColumnRenamed("_c96", "COBUYER_ID")
sourceDF =  sourceDF.withColumnRenamed("_c97", "COBUYER_SALUTATION")
sourceDF =  sourceDF.withColumnRenamed("_c98", "COBUYER_FIRST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c99","COBUYER_MIDDLE_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c100", "COBUYER_LAST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c101", "COBUYER_SUFFIX")
sourceDF =  sourceDF.withColumnRenamed("_c102", "COBUYER_FULL_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c103", "COBUYER_BIRTH_DATE")
sourceDF =  sourceDF.withColumnRenamed("_c104", "COBUYER_HOME_ADDR")
sourceDF =  sourceDF.withColumnRenamed("_c105", "COBUYER_ADDR_DISTRICT")
sourceDF =  sourceDF.withColumnRenamed("_c106", "COBUYER_ADDR_CITY")
sourceDF =  sourceDF.withColumnRenamed("_c107", "COBUYER_ADDR_REGION")
sourceDF =  sourceDF.withColumnRenamed("_c108", "COBUYER_ADDR_POSTAL_CODE")
sourceDF =  sourceDF.withColumnRenamed("_c109", "COBUYER_HOME_COUNTRY")
sourceDF =  sourceDF.withColumnRenamed("_c110", "COBUYER_HOME_PHONE_NUM")
sourceDF =  sourceDF.withColumnRenamed("_c111", "COBUYER_HOME_EXTENSION")
sourceDF =  sourceDF.withColumnRenamed("_c112", "COBUYER_HOME_PH_CNTRY_CODE")
sourceDF =  sourceDF.withColumnRenamed("_c113", "COBUYER_BUS_PH_NUM")
sourceDF =  sourceDF.withColumnRenamed("_c114", "COBUYER_BUS_EXTENSION")
sourceDF =  sourceDF.withColumnRenamed("_c115", "COBUYER_BUS_PH_CTRYCODE")
sourceDF =  sourceDF.withColumnRenamed("_c116", "COBUYER_EMAIL_ADDRESS")
sourceDF =  sourceDF.withColumnRenamed("_c117", "SALES_MGR_ID")
sourceDF =  sourceDF.withColumnRenamed("_c118", "SALES_MGR_SALUTATION")
sourceDF =  sourceDF.withColumnRenamed("_c119", "SALES_MGR_FIRST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c120", "SALES_MGR_MIDDLE_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c121", "SALES_MGR_LAST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c122", "SALES_MGR_SUFFIX")
sourceDF =  sourceDF.withColumnRenamed("_c123", "SALES_MGR_FULL_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c124", "FI_MGR_ID")
sourceDF =  sourceDF.withColumnRenamed("_c125", "FI_MGR_SALUTATION")
sourceDF =  sourceDF.withColumnRenamed("_c126", "FI_MGR_FIRST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c127", "FI_MGR_MIDDLE_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c128", "FI_MGR_LAST_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c129", "FI_MGR_SUFFIX")
sourceDF =  sourceDF.withColumnRenamed("_c130","FI_MGR_FULL_NAME")
sourceDF =  sourceDF.withColumnRenamed("_c131", "SALES_PEOPLE")
sourceDF =  sourceDF.withColumnRenamed("_c132", "TRADE_VEHICLES")
sourceDF =  sourceDF.withColumnRenamed("_c133", "ACCIDENT_HEALTH_COST")
sourceDF =  sourceDF.withColumnRenamed("_c134", "ACCIDENT_HEALTH_RESERVE")
sourceDF =  sourceDF.withColumnRenamed("_c135", "ACCI_HEALTH_COVERAGE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c136", "ACCI_HEALTH_PREMIUM")
sourceDF =  sourceDF.withColumnRenamed("_c137", "ACCI_HEALTH_RATE")
sourceDF =  sourceDF.withColumnRenamed("_c138", "ACCI_HEALTH_TERM_MONTHS")
sourceDF =  sourceDF.withColumnRenamed("_c139", "ACCI_HEALTH_PROVIDER")
sourceDF =  sourceDF.withColumnRenamed("_c140", "CREDIT_LIFE_COST")
sourceDF =  sourceDF.withColumnRenamed("_c141", "CREDIT_LIFE_RESERVE")
sourceDF =  sourceDF.withColumnRenamed("_c142", "CREDIT_LIFE_COVERAGE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c143", "CREDIT_LIFE_PREMIUM")
sourceDF =  sourceDF.withColumnRenamed("_c144", "CREDIT_LIFE_RATE")
sourceDF =  sourceDF.withColumnRenamed("_c145", "CREDIT_LIFE_TERM_MONTHS")
sourceDF =  sourceDF.withColumnRenamed("_c146", "CREDIT_LIFE_PROVIDER")
sourceDF =  sourceDF.withColumnRenamed("_c147", "GAP_COST")
sourceDF =  sourceDF.withColumnRenamed("_c148", "GAP_RESERVE")
sourceDF =  sourceDF.withColumnRenamed("_c149", "GAP_COVERAGE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c150", "GAP_PREMIUM")
sourceDF =  sourceDF.withColumnRenamed("_c151", "GAP_RATE")
sourceDF =  sourceDF.withColumnRenamed("_c152", "GAP_TERM_MONTHS")
sourceDF =  sourceDF.withColumnRenamed("_c153", "GAP_PROVIDER")
sourceDF =  sourceDF.withColumnRenamed("_c154", "LOSS_OF_EMP_COST")
sourceDF =  sourceDF.withColumnRenamed("_c155", "LOSS_OF_EMP_RESERVE")
sourceDF =  sourceDF.withColumnRenamed("_c156", "LOSS_OF_EMP_COVERAGE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c157", "LOSS_OF_EMP_PREMIUM")
sourceDF =  sourceDF.withColumnRenamed("_c158", "LOSS_OF_EMP_RATE")
sourceDF =  sourceDF.withColumnRenamed("_c159", "LOSS_OF_EMP_TERM_MONTHS")
sourceDF =  sourceDF.withColumnRenamed("_c160", "LOSS_OF_EMP_PROVIDER")
sourceDF =  sourceDF.withColumnRenamed("_c161", "MECH_BRKDN_INSURANC_COST")
sourceDF =  sourceDF.withColumnRenamed("_c162", "MBI_RESERVE")
sourceDF =  sourceDF.withColumnRenamed("_c163", "MBI_COVERAGE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c164", "MBI_PREMIUM")
sourceDF =  sourceDF.withColumnRenamed("_c165", "MBI_RATE")
sourceDF =  sourceDF.withColumnRenamed("_c166", "MBI_TERM_MONTHS")
sourceDF =  sourceDF.withColumnRenamed("_c167", "MBI_TERM_MILES")
sourceDF =  sourceDF.withColumnRenamed("_c168", "MBI_PROVIDER")
sourceDF =  sourceDF.withColumnRenamed("_c169", "SER_CONT_COST")
sourceDF =  sourceDF.withColumnRenamed("_c170", "SER_CONT_RESERVE")
sourceDF =  sourceDF.withColumnRenamed("_c171", "SER_CONT_COVERAGE_AMT")
sourceDF =  sourceDF.withColumnRenamed("_c172", "SER_CONT_PREMIUM")
sourceDF =  sourceDF.withColumnRenamed("_c173", "SER_CONT_RATE")
sourceDF =  sourceDF.withColumnRenamed("_c174", "SER_CONT_TERM_MONTHS")
sourceDF =  sourceDF.withColumnRenamed("_c175", "SER_CONT_TERM_MILES")
sourceDF =  sourceDF.withColumnRenamed("_c176", "SER_CONT_PROVIDER")
sourceDF
  }
    def getsourceDataFrame(sourceDF:DataFrame):DataFrame={
    
     
    var transactionsODS: DataFrame = null

    

      try {
        transactionsODS = Extractor.extract(Resource.AVRO, uri + Config.readNestedValue(config, "TARGET_PATH", "SALESTRANSACTIONS"))
        transactionsODS = DataOperations.getCurrentTransactions(transactionsODS, Seq("DLR_CODE", "VIN", "DEAL_NUMBER"))
       // logger.info("Count of existing Sales Transactions data = " + transactionsODS.count())
      } catch {
        case ex: Exception => {
          //logger.info("Existing Sales Transactions data not found.. Proceeding further with null")
          transactionsODS = null
        }
      }
      var defination = new DFDefinition()
      defination.appendODS(transactionsODS, true)
      defination.appendRules(uri + config("RULE_PATH"))
     defination.setRenameColumns(uri + config("RENAME_COLUMNS"))
      defination.addAttribute(Attribute.DATEFORMAT, config.getOrElse("DATE_FORMAT", "dd-MMM-yyyy"))
      defination.setStatusColumns(statusCols)
      defination.setSchema(uri + Config.readNestedValue(config, "SCHEMA", "SALESTRANSACTIONS"), Resource.JSON)

      var transformer = new VehicleSalesTransformer(defination)

     
     var outputDF = transformer.renameColumns(sourceDF)
      
      
      outputDF = transformer.applyCleansing(outputDF)
            
      print(outputDF.count)
      outputDF = transformer.markDuplicates(outputDF)

  
      outputDF = transformer.applyConversions(outputDF, Config.readNestedKey(config, "CONVERT_COLUMNS"))

      //logger.info("Denormalize Data..")
      outputDF = transformer.denormalizeData(outputDF, denormAttributes)

     print("Add Existing Purchase Order Date ..")
      outputDF = transformer.addExistingPODate(outputDF)

     print("Add Generic Columns ..")
      outputDF = transformer.addGenericColumns(outputDF)

     
       print("Applying Rules..")
      outputDF = transformer.applyRules(outputDF)
      outputDF=outputDF.filter(col("prc")===lit("SUCCESS"))
      //outputDF.show()
      print("Success Records")
    print(outputDF.count)
      outputDF
    }

   
}