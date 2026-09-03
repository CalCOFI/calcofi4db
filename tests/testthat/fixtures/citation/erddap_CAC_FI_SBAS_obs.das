Attributes {
 s {
  idnum {
    String description "unique ID number";
    String ioos_category "Other";
    String long_name "idnum";
  }
  gis_key {
    String ioos_category "Other";
  }
  behavior {
    String ioos_category "Other";
  }
  species {
    String ioos_category "Other";
  }
  count {
    Int32 actual_range 1, 12950;
    String ioos_category "Other";
  }
 }
  NC_GLOBAL {
    String cdm_data_type "Other";
    String Conventions "CF-1.6, ACDD-1.3, COARDS";
    String creator_email "sathompson@faralloninstitute.org";
    String creator_name "Sarah Ann Thompson";
    String data_sharing_agreement "https://oceanview.pfeg.noaa.gov/CalCOFI/app/resources/docs/Data_Sharing_Agreement_FarallonInstitute.pdf";
    String history "2026-09-03T17:01:25Z (local files)
2026-09-03T17:01:25Z https://oceanview.pfeg.noaa.gov/erddap/tabledap/CAC_FI_SBAS_obs.das";
    String infoUrl "https://oceanview.pfeg.noaa.gov/CalCOFI/calcofi_fi_sbas.html";
    String institution "Farallon Institute, Scripps Institution of Oceanography";
    String license "please see: https://oceanview.pfeg.noaa.gov/CalCOFI/app/resources/docs/Data_Sharing_Agreement_FarallonInstitute.pdf";
    String publisher_email "wsydeman@faralloninstitute.org";
    String publisher_name "William Sydeman";
    String sourceUrl "(local files)";
    String standard_name_vocabulary "CF Standard Name Table v70";
    String subsetVariables "idnum,gis_key,behavior,species,count";
    String summary "Please see: https://www.faralloninstitute.org/atsea-surveys and https://oceanview.pfeg.noaa.gov/CalCOFI/calcofi_fi_sbas.html";
    String title "CalCOFI Farallon Institute Seabirds: Observations";
  }
}
