import json
from base64 import b64encode
from base64 import b64decode
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
#from Cryptodome.Random import get_random_bytes
#from Cryptodome.Cipher import AES
import time, datetime
import math
import numpy as np  
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, LongType, DateType, StringType, StructType, StructField

# Initialize Spark Session
warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("Load CSV to generate schema") \
    .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
#sc.addPyFile("/zeppelin/.ivy2/jars/io.delta_delta-core_2.11-0.6.1.jar")
spark.sparkContext.addPyFile("/usr/local/spark/jars/io.delta_delta-core_2.11-0.6.1.jar")
from delta.tables import *

import pandas as pd

#Symmetric and Deterministic Cipher usage implementation
# N.B. cipher must be created and initiliased at point of use
# N.B.2 be careful with character encoding when transforming text or lists of text via various Python functions
class SeRPCryptoSD(object):
    _key = b"A thirty two byte encryption key"
    _header = b"header"                              # encryption "SALT"

    def __init__(self, key, header):
        self._key = key
        self._header = header

    def EncryptVal(self, ptValue):
        cipher = AES.new(self._key, AES.MODE_SIV)        
        cipher.update(self._header)
        cipherVal, tagVal = cipher.encrypt_and_digest(ptValue)

        cipherText = b64encode(cipherVal).decode('utf-8')
        tag = b64encode(tagVal).decode('utf-8')

        return cipherText, tag
        

    def DecryptVal(self, cipherText, tag):
        cipherVals = b64decode(cipherText)
        tagVals = b64decode(tag)

        cipher = AES.new(self._key, AES.MODE_SIV)
        cipher.update(self._header)
        plainText = cipher.decrypt_and_verify(cipherVals, tagVals)

        return plainText


    def EncryptList(self, encList):
        encryptedList = []
        encTagList = []

        for text in encList:
            byteArr = text.encode('utf-8')

            cipher = AES.new(self._key, AES.MODE_SIV)        
            cipher.update(self._header)
            encBytes, tagBytes = cipher.encrypt_and_digest(byteArr)

            encryptedList.append([b64encode(encBytes).decode('utf-8'), b64encode(bytes(byteArr)).decode('utf-8')])
        
        return encryptedList


    def DecryptList(self, list):
        decryptedList = []

        for enc in list:
            cipherVal = b64decode(enc[0])
            tagVal = b64decode(enc[1])

            cipher = AES.new(self._key, AES.MODE_SIV)
            cipher.update(self._header)

            decryptedList.append(cipher.decrypt_and_verify(cipherVal, tagVal).decode('utf-8'))

        return decryptedList


class SequenceMan(object):
    _sequenceFilename = ""
    
    def __init__(self, sequenceFilename = "/enc/KEY_XREF_SEQ"):
        self._sequenceFilename = sequenceFilename
        
    def CreateKeySeqTable(self):
        data = [{"NEXT_KEY_SEQ_NO": 1}]
        schema = StructType([StructField('NEXT_KEY_SEQ_NO', LongType(), nullable=False)])
        
        df = spark.createDataFrame(data, schema=schema)
        #df.show()
        df.write.format("delta").save(self._sequenceFilename)
        
    def GetNextSeqNo(self, increment = 1):
        df = spark.read.format("delta").load(self._sequenceFilename)
        nextVal = df.first()[0]
        deltaTable = DeltaTable.forPath(spark, self._sequenceFilename)
        updateExpr = "NEXT_KEY_SEQ_NO + {}".format(increment)
        deltaTable.update(set = { "NEXT_KEY_SEQ_NO": expr(updateExpr) })
        #deltaTable.update(set = { "NEXT_KEY_SEQ_NO": expr("NEXT_KEY_SEQ_NO + 1") })
        return nextVal

    # only for testing!!
    def PeekNextSeqNo(self):
        df = spark.read.format("delta").load(self._sequenceFilename)
        nextVal = df.first()[0]
        print("Next SequenceNo = {}".format(nextVal))

    # only for testing!!
    def ResetSeqNo(self, resetTo = 1):
        deltaTable = DeltaTable.forPath(spark, self._sequenceFilename)
        resetExpr = str(resetTo)
        deltaTable.update(set = { "NEXT_KEY_SEQ_NO": expr(resetExpr) })
        print("Sequence reset to {}".format(resetTo))


class KeyEncryption(object):
    _krFilename = "/enc/KEY_XREF_ZoeT"
    _keyValLen = 30
    _keySeqMan = SequenceMan("/enc/KEY_XREF_SEQ_ZoeT")
    _batchSize = 1000000
    _encTypeName = "KEY"
    _keyRefColName = "KEY_E"
    _encValColName = "KEY_E_ORI"
    _encTagColName = "KEY_E_ORI_TAG"

    def __init__(self, keyValLen):
        self._keyValLen = keyValLen
    
    
    def CreateKeyRefTable(self):
        # test creating and populating key reference table
        df1 = spark.createDataFrame([(0, "*Encrypted Value*","*TAG*")]).toDF(self._keyRefColName, self._encValColName, self._encTagColName)
        df1.write.format("delta").mode('overwrite').save(self._krFilename)
        print("Key refs table created")
        
        
    """
    filter for new key according to what's in existing key ref table, returns a data frame
    """
    def __filterNewKeys(self, encList):
        dfEncList = spark.createDataFrame(encList).toDF(self._encValColName, self._encTagColName)
        dfOld = spark.read.format("delta").load(self._krFilename)
        lref = "a." + self._encValColName
        rref = "b." + self._encValColName
        dfNewEncList = dfEncList.alias('a').join(dfOld.alias('b'), col(lref) == col(rref), 'left_anti')
        print("{} new key references".format(dfNewEncList.count()))
        return dfNewEncList
        
        
    def __GenKeyRefs1Batch(self, keyList):
        crypto = SeRPCryptoSD(b"A thirty two byte encryption key", b"header")
        keyPaddedList = [x.ljust(self._keyValLen) for x in keyList]
        encList = crypto.EncryptList(keyPaddedList)

        dfNewKeys = self.__filterNewKeys(encList)
        newKeys = [list(row) for row in dfNewKeys.collect()]

        noofNewKeys = len(newKeys)
        if(noofNewKeys > 0):
            initialKey = self._keySeqMan.GetNextSeqNo(noofNewKeys)
            keyRefs = []
            for i in range(0, noofNewKeys):
                keyRefs.append([initialKey + i, newKeys[i][0], newKeys[i][1]])
    
            df = spark.createDataFrame(keyRefs).toDF(self._keyRefColName, self._encValColName, self._encTagColName)
            df.write.format("delta").mode("append").save(self._krFilename)
        print("{} new key(s) added".format(noofNewKeys))
        

    def __GenKeyRefsNBatch(self, keyList):
        crypto = SeRPCryptoSD(b"A thirty two byte encryption key", b"header")
        keyPaddedList = [str(x).ljust(self._keyValLen) for x in keyList]
        encList = crypto.EncryptList(keyPaddedList)

        encListSize = len(encList)
        noofBatches = math.ceil(encListSize / self._batchSize)
        noofNewKeys = 0

        for iBatch in range(0, noofBatches):
            if iBatch < (noofBatches - 1):
                encListPart = encList[(iBatch * self._batchSize):(iBatch * self._batchSize + self._batchSize)]
            else:
                encListPart = encList[((noofBatches-1) * self._batchSize):encListSize]
    
            dfNewKeys = self.__filterNewKeys(encListPart)
            newKeys = [list(row) for row in dfNewKeys.collect()]

            noofNewKeys += len(newKeys)
            if(noofNewKeys > 0):
                initialKey = self._keySeqMan.GetNextSeqNo(len(newKeys))
                keyRefs = []
                for i in range(0, len(newKeys)):
                    keyRefs.append([initialKey + i, newKeys[i][0], newKeys[i][1]])
        
                df = spark.createDataFrame(keyRefs).toDF(self._keyRefColName, self._encValColName, self._encTagColName)
                df.write.format("delta").mode("append").save(self._krFilename)

        print("{} new key(s) added".format(noofNewKeys))


    def __GenKeyRefsNBatchPL(self, keyPaddedList):
        crypto = SeRPCryptoSD(b"A thirty two byte encryption key", b"header")
        encList = crypto.EncryptList(keyPaddedList)

        encListSize = len(encList)
        noofBatches = math.ceil(encListSize / self._batchSize)
        noofNewKeys = 0

        for iBatch in range(0, noofBatches):
            if iBatch < (noofBatches - 1):
                encListPart = encList[(iBatch * self._batchSize):(iBatch * self._batchSize + self._batchSize)]
            else:
                encListPart = encList[((noofBatches-1) * self._batchSize):encListSize]
    
            dfNewKeys = self.__filterNewKeys(encListPart)
            newKeys = [list(row) for row in dfNewKeys.collect()]

            noofNewKeys += len(newKeys)
            if(noofNewKeys > 0):
                initialKey = self._keySeqMan.GetNextSeqNo(len(newKeys))
                keyRefs = []
                for i in range(0, len(newKeys)):
                    keyRefs.append([initialKey + i, newKeys[i][0], newKeys[i][1]])
        
                df = spark.createDataFrame(keyRefs).toDF(self._keyRefColName, self._encValColName, self._encTagColName)
                df.write.format("delta").mode("append").save(self._krFilename)

        print("{} new key(s) added".format(noofNewKeys))


    def GenKeyRefs(self, keyList):
        #self.__GenKeyRefs1Batch(keyList)
        self.__GenKeyRefsNBatch(keyList)

    
    def GenKeyRefsDFPL(self, dfKeyList):
        print("GenKeyRefsDFPL")
        keyList = [r["KeyValPadded"] for r in dfKeyList.collect()]
        self.__GenKeyRefsNBatchPL(keyList)
    
    
    def GenKeyRefsDF(self, dfKeyList):
        # todo ensure list of keys to generate key refs for is distinct
        print("Generating key refs DF")
        spark_udf = udf(Paddy.PadValue, StringType())
        dfKeyPaddedList = dfKeyList.withColumn('KeyValPadded', spark_udf('KeyVal'))
        print("Padded List created")


    def ValidateKeyLen(self, keyList):
        n = len(keyList);
        i = 0
        while(i < n):
            keyLen = len(str(keyList[i]))
            if(keyLen > self._keyValLen):
                print("Key Value exceeds encryption type length [max = {}]: {}".format(self._keyValLen, keyList[i]))
                return False
            i += 1

        return True
        
        
    def ValidateKeyLenDF(self, dfKeyVals):
        dfKeyVals.createOrReplaceTempView("dfKeyVals")
        sql = "select max(length(KeyVal)) as KeyValMaxLen from dfKeyVals"
        dfRes = spark.sql(sql)
        keyValMaxLen = dfRes.select(col("KeyValMaxLen")).first()["KeyValMaxLen"]
        if(keyValMaxLen > self._keyValLen):
            if(keyLen > self._keyValLen):
                print("Key values were found which exceed the max length ({}) for this type of encryption".format(self._keyValLen))
                sqlSeek = "select KeyVal as KeyValMaxLen from dfKeyVals where max(length(KeyVal)) > {}".format(self._keyValLen)
                dfBadKeyVals = spark.sql(sql)
                dfBadKeyVals.show()
                return False
            
        return True
        
    def __PrepareEncLookup(self, keyList, encList):
        print("Preparing enc lookup ", datetime.datetime.now())
        batchSize = 20000000
        keyListSize = len(keyList)
        noofBatches =  math.ceil(keyListSize / batchSize)
        for iBatch in range(0, noofBatches):
            if iBatch < (noofBatches - 1):
                encLookupPart = [(keyList[i], encList[i][0]) for i in range(iBatch * batchSize, iBatch * batchSize + batchSize)]
            else:
                encLookupPart = [(keyList[i], encList[i][0]) for i in range((noofBatches-1) * batchSize, keyListSize)]

            dfEncLookupPart = spark.createDataFrame(encLookupPart).toDF(self._encTypeName, self._encValColName)
            if(iBatch > 0):
                dfEncLookup = dfEncLookup.union(dfEncLookupPart)
            else:
                dfEncLookup = dfEncLookupPart
                
        return dfEncLookup
                
        
    def FetchSubstitutions(self, keyList):
        crypto = SeRPCryptoSD(b"A thirty two byte encryption key", b"header")
        keyPaddedList = [str(x).ljust(self._keyValLen) for x in keyList]
        encList = crypto.EncryptList(keyPaddedList)
        dfEncLookup = self.__PrepareEncLookup(keyList, encList)
        
        dfExisting = spark.read.format("delta").load(self._krFilename)
        lref = "a." + self._encValColName
        rref = "b." + self._encValColName
        print("preparing subst lookup", datetime.datetime.now())
        dfSubstLookup = dfEncLookup.alias('a').join(dfExisting.alias('b'), col(lref) == col(rref), 'inner').select(col("b."+self._keyRefColName), col("b."+self._encValColName), col("a."+self._encTypeName))
        
        return dfSubstLookup
        

    def ShowKeys(self):
        df = spark.read.format("delta").load(self._krFilename)
        df.show()
        print("{} key references".format(df.count()))


class Paddy():
    def PadValueTo30(keyValue):
        return str(keyValue).ljust(30)

    def PadValueTo50(keyValue):
        return str(keyValue).ljust(50)


        
class KEY30Encryption(KeyEncryption):
    def __init__(self):
        self._krFilename = "/enc/KEY_XREF_ZoeT"
        self._keyValLen = 30
        self._keySeqMan = SequenceMan("/enc/KEY_XREF_SEQ_ZoeT")
        self._encTypeName = "KEY"
        self._keyRefColName = "KEY_E"
        self._encValColName = "KEY_E_ORI"
        self._encTagColName = "KEY_E_ORI_TAG"
    
    
class KEY50Encryption(KeyEncryption):
    def __init__(self):
        self._krFilename = "/enc/KEY_XREF_ZoeT"
        self._keyValLen = 50
        self._keySeqMan = SequenceMan("/enc/KEY_XREF_SEQ_ZoeT")
        self._encTypeName = "KEY"
        self._keyRefColName = "KEY_E"
        self._encValColName = "KEY_E_ORI"
        self._encTagColName = "KEY_E_ORI_TAG"
        

    def GenKeyRefsDF(self, dfKeyList):
        # todo ensure list of keys to generate key refs for is distinct
        print("Generating key refs DF")
        spark_udf = udf(Paddy.PadValueTo50, StringType())
        dfKeyPaddedList = dfKeyList.withColumn('KeyValPadded', spark_udf('KeyVal'))
        print("Padding applied")
        print(dfKeyPaddedList.count())
        #dfKeyPaddedList.show(10)     # show here causes a crash!!
        self.GenKeyRefsDFPL(dfKeyPaddedList)

    
class ALFEncryption(KeyEncryption):
    def __init__(self):
        self._krFilename = "/enc/ALF_XREF_ZoeT"
        self._keyValLen = 10
        self._keySeqMan = SequenceMan("/enc/ALF_XREF_SEQ_ZoeT")
        self._encTypeName = "ALF"
        self._keyRefColName = "ALF_E"
        self._encValColName = "ALF_E_ORI"
        self._encTagColName = "ALF_E_ORI_TAG"


class HCPEncryption(KeyEncryption):
    def __init__(self):
        self._krFilename = "/enc/HCP_XREF_ZoeT"
        self._keyValLen = 12
        self._keySeqMan = SequenceMan("/enc/HCP_XREF_SEQ_ZoeT")
        self._encTypeName = "HCP"
        self._keyRefColName = "HCP_E"
        self._encValColName = "HCP_E_ORI"
        self._encTagColName = "HCP_E_ORI_TAG"
    
    
class RALFEncryption(KeyEncryption):
    def __init__(self):
        self._krFilename = "/enc/RALF_XREF_ZoeT"
        self._keyValLen = 16
        self._keySeqMan = SequenceMan("/enc/RALF_XREF_SEQ_ZoeT")
        self._encTypeName = "RALF"
        self._keyRefColName = "RALF_E"
        self._encValColName = "RALF_E_ORI"
        self._encTagColName = "RALF_E_ORI_TAG"

        
class TableEncryption(object):
    _tmpViewPrefix = "kl"
    _defaultDTViewName = "dataTable"
    
    
    def __GetValsToEnc(self, dfTable, encFields):
        # Warning.  too many key values can cause this error:
        # org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of 16 tasks (1067.3 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
        # see https://stackoverflow.com/questions/47996396/total-size-of-serialized-results-of-16-tasks-1048-5-mb-is-bigger-than-spark-dr/47999105
        valsToEnc = []
        for i in range(0, len(encFields)):
            valsToEnc.extend([r[encFields[i]] for r in dfTable.collect()])
            # TODO - test quicker methods
        valsToEnc = list(set(valsToEnc))    # make distinct
        return valsToEnc
    

    def __GetValsToEncDF(self, dfTable, encFields):
        dfTable.createOrReplaceTempView("dfTable")
        valsToEnc = []
        sql = ""
        for i in range(0, len(encFields)):
            if(i > 0): sql += "union\n"
            sql += "select distinct {} as KeyVal from dfTable\n".format(encFields[i])

        dfValsToEnc = spark.sql(sql)
        return dfValsToEnc

    
    def __EncryptKey30(self, dfTable, encFields):
        # TODO - try using Spark SQL to validate key len for more performance
        #print("KEY30 encryption for: {}".format(encFields))
        valsToEnc = self.__GetValsToEnc(dfTable, encFields)

        kEnc = KEY30Encryption()
        if(kEnc.ValidateKeyLen(valsToEnc)):
            kEnc.GenKeyRefs(valsToEnc)


    def __EncryptKey50(self, dfTable, encFields):
        print("KEY50 encryption for: {}".format(encFields))
        dfValsToEnc = self.__GetValsToEncDF(dfTable, encFields)
        #dfValsToEnc.show()
        kEnc = KEY50Encryption()
        if(kEnc.ValidateKeyLenDF(dfValsToEnc)):
            kEnc.GenKeyRefsDF(dfValsToEnc)


    def __EncryptALF(self, dfTable, encFields):
        print("ALF encryption for: {}".format(encFields))
        valsToEnc = self.__GetValsToEnc(dfTable, encFields)
        
        aEnc = ALFEncryption()
        if(aEnc.ValidateKeyLen(valsToEnc)):
            aEnc.GenKeyRefs(valsToEnc)


    def __EncryptHCP(self, dfTable, encFields):
        print("HCP encryption for: {}".format(encFields))
        valsToEnc = self.__GetValsToEnc(dfTable, encFields)
        
        hEnc = HCPEncryption()
        if(hEnc.ValidateKeyLen(valsToEnc)):
            hEnc.GenKeyRefs(valsToEnc)


    def __EncryptRALF(self, dfTable, encFields):
        print("RALF encryption for: {}".format(encFields))
        valsToEnc = self.__GetValsToEnc(dfTable, encFields)
        
        hEnc = RALFEncryption()
        if(hEnc.ValidateKeyLen(valsToEnc)):
            hEnc.GenKeyRefs(valsToEnc)

    
    def __FetchKey30Subst(self, dfTable, fieldname):
        print("fetching KEY30 substitutions for {}".format(fieldname))
        distinct_ids = dfTable.select(fieldname).distinct().rdd.map(lambda r: r[0]).collect()

        kEnc = KEY30Encryption()
        dfSubstLookup = kEnc.FetchSubstitutions(distinct_ids)
        #dfSubstLookup.show()
        tmpViewName = "{}_{}".format(self._tmpViewPrefix, fieldname)
        dfSubstLookup.createOrReplaceTempView(tmpViewName)
        return dfSubstLookup


    def __FetchKey50Subst(self, dfTable, fieldname):
        print("fetching KEY50 substitutions for {}".format(fieldname))
        #this method will give this error on large result sets

        dfTable.createOrReplaceTempView("dfTable")
        sql = "select distinct {} from dfTable".format(fieldname)
        dfDistinctIDs = spark.sql(sql)
        distinct_ids = [r[fieldname] for r in dfDistinctIDs.collect()]
        print("distinct_ids count = {}".format(len(distinct_ids)))
        
        kEnc = KEY50Encryption()
        # TODO - test fetching subs within spark context for performance
        dfSubstLookup = kEnc.FetchSubstitutions(distinct_ids)
        #dfSubstLookup.show()
        tmpViewName = "{}_{}".format(self._tmpViewPrefix, fieldname)
        dfSubstLookup.createOrReplaceTempView(tmpViewName)
        return dfSubstLookup


    def __FetchALFSubst(self, dfTable, fieldname):
        print("fetching ALF substitutions for {}".format(fieldname))
        distinct_ids = dfTable.select(fieldname).distinct().rdd.map(lambda r: r[0]).collect()
        #print(distinct_ids)
        
        aEnc = ALFEncryption()
        dfSubstLookup = aEnc.FetchSubstitutions(distinct_ids)
        #dfSubstLookup.show()
        tmpViewName = "{}_{}".format(self._tmpViewPrefix, fieldname)
        dfSubstLookup.createOrReplaceTempView(tmpViewName)
        return dfSubstLookup


    def __FetchHCPSubst(self, dfTable, fieldname):
        print("fetching HCP substitutions for {}".format(fieldname))
        distinct_ids = dfTable.select(fieldname).distinct().rdd.map(lambda r: r[0]).collect()
        #print(distinct_ids)
        
        hEnc = HCPEncryption()
        dfSubstLookup = hEnc.FetchSubstitutions(distinct_ids)
        #dfSubstLookup.show()
        tmpViewName = "{}_{}".format(self._tmpViewPrefix, fieldname)
        dfSubstLookup.createOrReplaceTempView(tmpViewName)
        return dfSubstLookup


    def __FetchRALFSubst(self, dfTable, fieldname):
        print("fetching RALF substitutions for {}".format(fieldname))
        distinct_ids = dfTable.select(fieldname).distinct().rdd.map(lambda r: r[0]).collect()
        #print(distinct_ids)
        
        rEnc = RALFEncryption()
        dfSubstLookup = rEnc.FetchSubstitutions(distinct_ids)
        #dfSubstLookup.show()
        tmpViewName = "{}_{}".format(self._tmpViewPrefix, fieldname)
        dfSubstLookup.createOrReplaceTempView(tmpViewName)
        return dfSubstLookup

    
    def __LOLToDict(self, lol):
        dict = {}
        for i in range(0, len(lol)):
            for j in range(0, len(lol[i][1])):
                dict.update({lol[i][1][j] : lol[i][0]})
        print(dict)
        return dict
    
    
    def __buildSubstSQL(self, dfTable, encSets):
        substSQL = "select\n"
    
        encDict = self.__LOLToDict(encSets)
        
        firstCol = True
        for fieldname in dfTable.schema.names:
            substSQL += "\t"
            if(firstCol is not True): substSQL += ", "
            
            encType = encDict.get(fieldname)
            if(encType == "KEY" or encType == "KEY50"):
                substSQL += "{0}_{1}.KEY_E as {1}_E".format(self._tmpViewPrefix, fieldname)
            elif(encType == "ALF"):
                substSQL += "{0}_{1}.ALF_E as {1}_E".format(self._tmpViewPrefix, fieldname)
            elif(encType == "HCP"):
                substSQL += "{0}_{1}.HCP_E as {1}_E".format(self._tmpViewPrefix, fieldname)
            elif(encType == "RALF"):
                substSQL += "{0}_{1}.RALF_E as {1}_E".format(self._tmpViewPrefix, fieldname)
            else:
                substSQL += fieldname
            
            substSQL += "\n"
            firstCol = False
        
        substSQL += "\tfrom {}\n".format(self._defaultDTViewName)
        
        for i in range(0, len(encSets)):
            if (encSets[i][0] == "KEY" or  encSets[i][0] == "KEY50"):
                for j in range(0, len(encSets[i][1])):
                    substSQL += "\t\tleft join {0}_{1} on {2}.{3} = {0}_{1}.KEY\n".format(self._tmpViewPrefix, encSets[i][1][j], self._defaultDTViewName, encSets[i][1][j])
            #elif encSets[i][0] == "KEY50": 
            #    for j in range(0, len(encSets[i][1])):
            #        substSQL += "\t\tleft join {0}_{1} on {2}.{3} = {0}_{1}.KEY\n".format(self._tmpViewPrefix, encSets[i][1][j], self._defaultDTViewName, encSets[i][1][j])
            elif encSets[i][0] == "ALF": 
                for j in range(0, len(encSets[i][1])):
                    substSQL += "\t\tleft join {0}_{1} on {2}.{3} = {0}_{1}.ALF\n".format(self._tmpViewPrefix, encSets[i][1][j], self._defaultDTViewName, encSets[i][1][j])
            elif encSets[i][0] == "HCP": 
                for j in range(0, len(encSets[i][1])):
                    substSQL += "\t\tleft join {0}_{1} on {2}.{3} = {0}_{1}.HCP\n".format(self._tmpViewPrefix, encSets[i][1][j], self._defaultDTViewName, encSets[i][1][j])
            elif encSets[i][0] == "RALF": 
                for j in range(0, len(encSets[i][1])):
                    substSQL += "\t\tleft join {0}_{1} on {2}.{3} = {0}_{1}.RALF\n".format(self._tmpViewPrefix, encSets[i][1][j], self._defaultDTViewName, encSets[i][1][j])

        substSQL += "\n"
        
        return substSQL
        

    def EncryptTable(self, dfTable, encSets):
        print("Encrypting table")
        
        # 1 generate key refs
        for i in range(0, len(encSets)):
            if encSets[i][0] == "KEY": self.__EncryptKey30(dfTable, encSets[i][1])
            elif encSets[i][0] == "KEY50": self.__EncryptKey50(dfTable, encSets[i][1])
            elif encSets[i][0] == "ALF": self.__EncryptALF(dfTable, encSets[i][1])
            elif encSets[i][0] == "HCP": self.__EncryptHCP(dfTable, encSets[i][1])
            elif encSets[i][0] == "RALF": self.__EncryptRALF(dfTable, encSets[i][1])
        
        # 2 key subsitutions
        print("Substituting keys")
        
        # 2a fetch key subsitutions
        for i in range(0, len(encSets)):
            if encSets[i][0] == "KEY": 
                for j in range(0, len(encSets[i][1])):
                    self.__FetchKey30Subst(dfTable, encSets[i][1][j])
            elif encSets[i][0] == "KEY50": 
                for j in range(0, len(encSets[i][1])):
                    self.__FetchKey50Subst(dfTable, encSets[i][1][j])
            elif encSets[i][0] == "ALF": 
                for j in range(0, len(encSets[i][1])):
                    self.__FetchALFSubst(dfTable, encSets[i][1][j])
            elif encSets[i][0] == "HCP": 
                for j in range(0, len(encSets[i][1])):
                    self.__FetchHCPSubst(dfTable, encSets[i][1][j])
            elif encSets[i][0] == "RALF":
                for j in range(0, len(encSets[i][1])):
                    self.__FetchRALFSubst(dfTable, encSets[i][1][j])

        # 2b substitute keys
        dfTable.createOrReplaceTempView("dataTable")
        substSQL = self.__buildSubstSQL(dfTable, encSets)
        #print(substSQL)
        return spark.sql(substSQL)
        
        
    def DecryptTable(self, dfTable, encSets):
        print("Table Decryption - under construction")

df_patients_df = spark.read.format("parquet").load("/stage/CVST/20210110/df_assessments.parquet").repartition(3000)
print( df_patients_df.rdd.getNumPartitions())
df_patients_df.show(n=5)
encSets = [["KEY50",["Id"]]]
    #encSets = [["KEY50",["Id"]]]
    #encSets = [["ALF", ["ALF"]]]
tabEnc = TableEncryption()
df_patients_df_enc = tabEnc.EncryptTable(df_patients_df, encSets)
df_patients_df_enc.write.mode('overwrite').option("overwriteSchema", "true").format("delta").save("/tmp/jeff_df_assessments_enc.parquet")

   

 