/**
 * This script processes the FEC Candidates and Committees
 * and creates a pipe delimited file that we can load into Neo4J
 *
 * The data comes from: www.fec.gov
 */

candidates = LOAD 's3n://FEC/2012Data/2012CandidateTab.txt' USING PigStorage('|') AS 
(candidate_id:chararray,candidate_name:chararray,candidate_party:chararray,election_year:chararray, 
    candidate_office_state:chararray,candidateOffice:chararray,candidateDistrict:chararray, 
    candidate_ici:chararray,candidateStatus:chararray, committee_id:chararray,
	candStreet1:chararray,candStreet2:chararray,candCity:chararray,candState:chararray, candZip_code:chararray);

committees = LOAD 's3n://FEC/2012Data/2012committee.dat' USING PigStorage('|') AS (
    committee_id:chararray,committee_name:chararray,treasurer:chararray,address1:chararray,address2:chararray,city:chararray,
    state:chararray,zip_code:chararray,committee_designation:chararray,committee_type:chararray,committee_party:chararray,
    committee_filing_freq:chararray,interest_group_org:chararray,connected_org_name:chararray,candidate_id:chararray);

subCandidate = foreach candidates generate candidate_id, candidate_name, candidate_name as name, candidate_party, election_year, candidate_office_state,candidateOffice,candidateDistrict,candidate_ici,
candidateStatus,committee_id,candStreet1, candStreet2, candCity,candState,candZip_code, 'na','na', CONCAT(CONCAT(SUBSTRING (candidate_name,INDEXOF(candidate_name,',',0)+1,100),' '),SUBSTRING(candidate_name,0,INDEXOF(candidate_name,',',0))), addLink(candidate_party);

subCommittee = foreach committees generate committee_id, committee_name, committee_name as name, treasurer, address1, address2, city, state, zip_code, committee_designation, committee_type, committee_party, committee_filing_freq,
interest_group_org, connected_org_name, candidate_id, 'na','na';

rmf s3n://Mortar/MortarData;
STORE subCandidate INTO 's3n://Mortar/MortarData/candidates' USING PigStorage('|');
STORE subCommittee INTO 's3n://Mortar/MortarData/committees' USING PigStorage('|');