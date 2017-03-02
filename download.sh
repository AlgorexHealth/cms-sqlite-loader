HTTP_FRONT="https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/"
OTHER_FRONT="http://downloads.cms.gov/files/"


mkdir -p sample_$1
cd sample_$1
curl  -O -L ${HTTP_FRONT}DE1_0_2008_Beneficiary_Summary_File_Sample_$1.zip  
  unzip DE1_0_2008_Beneficiary_Summary_File_Sample_$1.zip 
  rm DE1_0_2008_Beneficiary_Summary_File_Sample_$1.zip 
curl  -O -L ${OTHER_FRONT}DE1_0_2008_to_2010_Carrier_Claims_Sample_$1A.zip   
  unzip DE1_0_2008_to_2010_Carrier_Claims_Sample_$1A.zip
  rm DE1_0_2008_to_2010_Carrier_Claims_Sample_$1A.zip
curl  -O -L ${OTHER_FRONT}DE1_0_2008_to_2010_Carrier_Claims_Sample_$1B.zip   
  unzip DE1_0_2008_to_2010_Carrier_Claims_Sample_$1B.zip
  rm DE1_0_2008_to_2010_Carrier_Claims_Sample_$1B.zip
curl  -O -L ${HTTP_FRONT}DE1_0_2008_to_2010_Inpatient_Claims_Sample_$1.zip  
  unzip DE1_0_2008_to_2010_Inpatient_Claims_Sample_$1.zip
  rm DE1_0_2008_to_2010_Inpatient_Claims_Sample_$1.zip
curl  -O -L ${HTTP_FRONT}DE1_0_2008_to_2010_Outpatient_Claims_Sample_$1.zip 
  unzip DE1_0_2008_to_2010_Outpatient_Claims_Sample_$1.zip
  rm DE1_0_2008_to_2010_Outpatient_Claims_Sample_$1.zip
curl  -O -L ${OTHER_FRONT}DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_$1.zip
  unzip DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_$1.zip 
  rm DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_$1.zip 
curl  -O -L ${HTTP_FRONT}DE1_0_2009_Beneficiary_Summary_File_Sample_$1.zip  
  unzip DE1_0_2009_Beneficiary_Summary_File_Sample_$1.zip
  rm DE1_0_2009_Beneficiary_Summary_File_Sample_$1.zip
curl  -O -L ${HTTP_FRONT}DE1_0_2010_Beneficiary_Summary_File_Sample_$1.zip  
  unzip DE1_0_2010_Beneficiary_Summary_File_Sample_$1.zip
  rm DE1_0_2010_Beneficiary_Summary_File_Sample_$1.zip
