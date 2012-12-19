/**
 * This script processes the FEC Individual Contributions
 * and creates a pipe delimited file that we can load into Neo4J
 *
 * The data comes from: www.fec.gov
 */

indiv_cont = LOAD 's3n://FEC/2012Data/2012Individual.dat' USING PigStorage('|') AS (
    committee_id:chararray,amendment_ind:chararray,report_type:chararray,trans_pgi:chararray,
    image_num:chararray,transaction_type:chararray,entity_type:chararray,contributor_name:chararray,contrib_city:chararray,
    contrib_state:chararray,contrib_zip_code:chararray,employer:chararray,occupation:chararray, trans_date:chararray,
    transaction_amt:float,other_id:chararray,tran_id:chararray,file_num:chararray,
    memo_cd:chararray,memo_text:chararray,sub_id:chararray);

subIndiv = foreach indiv_cont generate CONCAT(CONCAT(contributor_name,contrib_city),(CONCAT(contrib_state,contrib_zip_code))) as contribID, contributor_name, contrib_city, contrib_state,contrib_zip_code, employer, occupation,'na','na';

grpdIndiv  = DISTINCT subIndiv;   --output a bag upped for each value of symbol
--take a bag of integers, produce one result for each group

allIndiv = foreach indiv_cont generate CONCAT(CONCAT(contributor_name,contrib_city),(CONCAT(contrib_state,contrib_zip_code))) as contribID, committee_id,trans_date, CONCAT(SUBSTRING(trans_date,4,8),CONCAT(SUBSTRING(trans_date,0,2),SUBSTRING(trans_date,2,4))) as flippedDate, transaction_amt, transaction_type, tran_id, file_num;


rmf s3n://Mortar/MortarData;
STORE grpdIndiv INTO 's3n://Mortar/MortarData/indivContribs' USING PigStorage('|');
STORE allIndiv INTO 's3n://Mortar/MortarData/allIndivContribs' USING PigStorage('|');
