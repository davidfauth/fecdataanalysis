/**
 * This script processes the creates the files for a JSON force graph solution
 * I had to cheat and use the uniqueCommittees file that I previously created
 *
 * The data comes from: Sunlight Labs
 */

-- Register our fecdatautil.py python UDF file
REGISTER '../udfs/python/fecdataanalysis.py' USING streaming_python AS fecdatautil;

pac_expend = LOAD 's3n://FEC/2012Data/allExpendituresTab.txt' USING PigStorage('|') AS (
 filler:chararray,spending_Committee:chararray,spending_committee_id:chararray,is_superpac:chararray,
 election_type:chararray,candidate_supp_opp:chararray,support_oppose_flag:chararray,
 candidate_id:chararray,candidate_party:chararray,candidate_office:chararray,
 candidate_district:chararray,candidate_state:chararray,expenditure_amt:float,expenditure_state:chararray,
 expenditure_date:chararray,election_type_two:chararray,recipient:chararray,purpose:chararray,
 transaction_id:chararray,filing_number:chararray);

uniqueCommitteeExpend = LOAD 's3n://FEC/2012Data/uniqueCommittees.txt' USING PigStorage('|') AS (
    committeeID:chararray,spending_Committee:chararray);

clean_pac_expend = foreach pac_expend generate filler, REPLACE(spending_Committee,'\\"','') as newCommittee, 
spending_committee_id, is_superpac, election_type, UPPER(REPLACE(candidate_supp_opp, '\\"', '')) as candidateName,
support_oppose_flag,candidate_id, candidate_party, candidate_office, candidate_district,
candidate_state, expenditure_amt, expenditure_date, election_type_two, recipient,
 REPLACE(purpose,'\\"','') as newPurpose, transaction_id, filing_number,  'na';
 
filtered_pac_expend = FILTER clean_pac_expend BY expenditure_date > '20120930' and (candidateName == 'OBAMA, BARACK' or candidateName == 'ROMNEY, MITT');
 
/* Join Committees with Expenditure*/
filtered_comm_cand = foreach filtered_pac_expend generate newCommittee, candidateName, support_oppose_flag, expenditure_amt;
GroupedCommitteeCand = GROUP filtered_comm_cand BY (newCommittee, candidateName);
byCommCand = FOREACH GroupedCommitteeCand GENERATE
    FLATTEN(group) AS (newCommittee, candidateName), SUM(filtered_comm_cand.expenditure_amt) as totalExpended, filtered_comm_cand.support_oppose_flag;

joinedExpend = join byCommCand by newCommittee, uniqueCommitteeExpend by spending_Committee;

linkedNodes = FOREACH joinedExpend GENERATE fecdatautil.buildLinkNodeString(committeeID, fecdatautil.buildEndNode(candidateName), totalExpended);
 
/* Get tuples of Committee Name and Purpose */ 
 filtered_pac_detail = foreach filtered_pac_expend generate newCommittee, fecdatautil.generateNodeType(CONCAT(CONCAT(support_oppose_flag,'_'),candidateName)) as commPurpose;
 
/* Group together identical tuples */
GroupedCommitteePurpose = GROUP filtered_pac_detail BY (newCommittee, commPurpose);

by_age_color_counts = FOREACH GroupedCommitteePurpose GENERATE
    FLATTEN(group) AS (newCommittee, commPurpose);

GroupCommPurpose = GROUP by_age_color_counts by newCommittee;
bycommPurposeSum = FOREACH GroupCommPurpose GENERATE
    FLATTEN(group) AS (newCommittee), SUM(by_age_color_counts.commPurpose) as purposeSum;

ordered = ORDER bycommPurposeSum BY newCommittee;

/* output the committees */
uniqueCommittee = FOREACH ordered GENERATE 'na', newCommittee;
/* output the nodes */
strNode = FOREACH ordered GENERATE fecdatautil.buildNodeString(newCommittee, purposeSum);
  
  
  
rmf s3n://Mortar/SuperPac;
--STORE pac_geo INTO 's3n://Mortar/SuperPac/pac_expend_geo' USING PigStorage('|');
STORE strNode INTO 's3n://Mortar/SuperPac/OctNovExpend' USING PigStorage('|');
STORE uniqueCommittee INTO 's3n://Mortar/SuperPac/UniqueCommittee' USING PigStorage('|');
STORE linkedNodes INTO 's3n://Mortar/SuperPac/LinkedNodes' USING PigStorage('|');
