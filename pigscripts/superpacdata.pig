/**
 * This script processes the SuperPac Expenditures and SuperPac Contributions
 * and creates a pipe delimited file that we can load into Neo4J
 *
 * The data comes from: http://reporting.sunlightfoundation.com/outside-spending/super-pacs/
 * This filters the SuperPac expenditures looking for expenditures after Sep 30, 2012
 */

pac_expend = LOAD 's3n://FEC/2012Data/allExpendituresTab.txt' USING PigStorage('|') AS (
 filler:chararray,spending_Committee:chararray,spending_committee_id:chararray,is_superpac:chararray,
 election_type:chararray,candidate_supp_opp:chararray,support_oppose_flag:chararray,
 candidate_id:chararray,candidate_party:chararray,candidate_office:chararray,
 candidate_district:chararray,candidate_state:chararray,expenditure_amt:float,expenditure_state:chararray,
 expenditure_date:chararray,election_type_two:chararray,recipient:chararray,purpose:chararray,
 transaction_id:chararray,filing_number:chararray);

pac_donors = LOAD 's3n://FEC/2012Data/allContribs.dta' USING PigStorage('|') AS (
 donor_type:chararray,commName:chararray,commID:chararray,donating_organization:chararray,
 donor_last:chararray,donor_first:chararray,donor_city:chararray,donor_state:chararray,
 donor_occupation:chararray,employer:chararray,amount:float,donationDate:chararray,total_amt_given_pac:float,
 transaction_id:chararray,filing_number:chararray);

filter_pac_expend = foreach pac_expend generate filler, REPLACE(spending_Committee,'\\"','') as newCommittee, 
spending_committee_id, is_superpac, election_type, UPPER(REPLACE(candidate_supp_opp, '\\"', '')) as candidateName,support_oppose_flag,candidate_id, candidate_party, candidate_office, candidate_district,
candidate_state, expenditure_amt, expenditure_date, election_type_two, recipient,
REPLACE(purpose,'\\"','') as newPurpose, transaction_id, filing_number, 'na';
 
filtered_pac_expend = FILTER filter_pac_expend BY expenditure_date > '20120930';

 
filter_cand_name = foreach filter_pac_expend generate filler, newCommittee,spending_committee_id, is_superpac, election_type,  CONCAT(CONCAT(SUBSTRING(candidateName,INDEXOF(candidateName,',',0)+1, 100),' '),SUBSTRING(candidateName,0,INDEXOF(candidateName,',',0))) as newCandName,support_oppose_flag,candidate_id, candidate_party, candidate_office, candidate_district,
candidate_state, expenditure_amt, expenditure_date, election_type_two, recipient,newPurpose, transaction_id, filing_number, 'na';
 
filter_pac_donors = foreach pac_donors generate donor_type, REPLACE(commName,'\\"','') as newReceivingSuperPac,
    commID,REPLACE(donating_organization,'\\"','') as newDonatingOrganization, donor_last,
    donor_first, donor_city,donor_state, REPLACE(donor_occupation,'\\"','') as newDonorOccupation, REPLACE(employer,'\\"','') as newEmployer, amount, donationDate, total_amt_given_pac, transaction_id, filing_number, 
    CONCAT(CONCAT(UCFIRST(LOWER(donor_first)),' '),UCFIRST(LOWER(donor_last))) as donorFullName, 'na','na';


rmf s3n://Mortar/SuperPac;
--STORE pac_geo INTO 's3n://Mortar/SuperPac/pac_expend_geo' USING PigStorage('|');
STORE filtered_pac_expend INTO 's3n://Mortar/SuperPac/OctNovExpend' USING PigStorage('|');
STORE filter_cand_name INTO 's3n://Mortar/SuperPac/superPacExpend' USING PigStorage('|');
STORE filter_pac_donors INTO 's3n://Mortar/SuperPac/superPacDonors' USING PigStorage('|');