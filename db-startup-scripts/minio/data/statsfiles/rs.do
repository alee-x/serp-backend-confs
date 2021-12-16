**********************************************
*** Run all Tables and Graphs in the Paper ***
**********************************************

set more off
clear*
set maxvar 10000

* set folder path 
global root "root_directory_path"

* force Stata to use the rdrobust version in this replication package
adopath ++ "$root/rdrobust_version_8.0.2"
adopath ++ "$root/rddensity_version_2.1"

di "$root"
cd "$root/codes/"


* Create the final dataset
qui do "process/create_final_sample.do"


* Table 1 (Summary Statistics)
qui do "analysis/summary_stats.do"

* Table 2 (Covariate Balance)
qui do "analysis/covariate_balance.do"

* Table 3 (Effect of a left-wing mayor)
qui do "analysis/aggregate_results.do"

* Table 4 (Bootstrap inference on differential effects) (warning: this takes long time to run)
qui do "analysis/bootstrap_subsample_differences.do"


* Figure 1 (RD graphs - whole sample)
qui do "analysis/rd_graphs.do"

* Figure 2 (Effect on social spending by year in office)
qui do "analysis/dynamic_graph.do"

* Figures 3 and Appendix Figure H.1 (Placebo thresholds - whole sample and subsamples)
qui do "analysis/placebo_cutoffs.do"

* Figure 4 (RD on coalition ideology score)
qui do "analysis/compute_average_ideological_distance.do"

* Figure 5 (RD graphs - mechanisms' subsamples)
qui do "analysis/rd_graphs_subsamples.do"

* Appendix Table B.1 (Covariates Summary Statistics)
qui do "analysis/summary_stats_covariates.do"

*Appendix Table C.1 (Covariate Balance Subsamples)
qui do "analysis/covariate_balance_subsample.do"

* Appendix Figure C.1 (Density of the running variable)
qui do "analysis/manipulation_tests.do"

* Appendix Figure C.2 (Density of the incumbent mayor share)
qui do "analysis/incumbent_manipulation_test.do"

* Appendix Figure C.3 (Density of the incumbent party share)
qui do "analysis/incumbent_party_manipulation_test.do"

* Appendix Figure D.1 (Candidate characteristics)
qui do "process/create_final_candidate_characteristics.do"
qui do "analysis/candidate_characteristics_test.do"

* Appendix Table E.1 (Revenue Composition)
qui do "analysis/aggregate_results_rev_detailed.do"

* Appendix Table F.1 (Dynamic effects and pre-trends - whole sample)
qui do "analysis/dynamic_results_baseline.do"

* Appendix Table F.2 (Dynamic effects and pre-trends - lameduck mayors subsample)
qui do "analysis/dynamic_results_lameduck.do"

* Appendix Table F.3 (Dynamic effects and pre-trends - oil windfalls subsample)
qui do "analysis/dynamic_results_oil.do"

* Appendix Figure G.1 (Effect on social spending by mayoral term)
qui do "process/create_extended_final_sample.do"
qui do "analysis/extended_sample_exercise.do"


* Appendix Table H.1 (Effect of a left-wing mayor, using differenced outcomes)
qui do "analysis/robustness_differenced_outcomes.do"

* Appendix Table H.2 (Effect of a left-wing mayor, excluding first year in office)
qui do "analysis/aggregate_results_3y_avg.do"

* Appendix Table H.3 (Effect of a left-wing mayor, by city size)
qui do "analysis/aggregate_results_by_city_size.do"

* Appendix Table H.4 (Effect of a left-wing mayor, alternative bandwidth)
qui do "analysis/aggregate_results_kernel_alternative.do"

* Appendix Table H.5 (Effect of a left-wing mayor, alternative tiebout and coalition score thresholds)
qui do "analysis/75pct_appendix.do"

* Appendix Table I.1 (Welfare outcomes summary statistics)
qui do "analysis/summary_stats_outcomes.do"

* Appendix Table I.2 (Effect on welfare-relevant outcomes)
qui do "analysis/aggregate_results_outcomes.do"

* Restore Stata to use the rdrobust version of the system
adopath - "$root/rdrobust_version_8.0.2"
adopath - "$root/rddensity_version_2.1"
