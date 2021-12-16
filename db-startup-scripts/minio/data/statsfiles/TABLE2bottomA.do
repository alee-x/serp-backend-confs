clear
set more off
program drop _all
program define sample, rclass

        syntax, alpha(real) studies(integer) select(real) obs(integer)

        // Remove existing variables
        drop _all

		//We first create the matrix to store the results of each study
		matrix A = J(`studies',1,0)
		matrix B = J(`studies',1,0)
		matrix C = J(`studies',1,0)
		
		forvalues i = 1/`studies' {
		clear
        // STEP ONE: Create the data for each study and estimate an effect
		set obs `obs'
		generate x = rnormal()
		// Note that each "study" has the same number of observations (100)
		// but differ in the variance of their respective error terms.  This
		// causes the estimate of the effect to be estimated with varying degrees 
		// of precision
		// This is for random effects
			scalar lambda = 0.5+30*runiform()
			generate e = lambda*rnormal()
			scalar alpha = `alpha' + rnormal()
			generate y = 1 + alpha*x + e
		quietly regress y x 
		scalar coef = _b[x]
		scalar secoef = _se[x]
		scalar tcoef = coef/secoef
				
		// First run this program once to get the pre-publication study sample data
		// To get post-publication study sample data, uncomment one of the two sections
		// below.
		
		/*if abs(tcoef) < 2 {
			 scalar dummy = cond(runiform()<`select',1,.)
			 scalar coef = dummy*coef
			 scalar secoef = dummy*secoef
			 scalar tcoef = dummy*tcoef
		}
		*/

        if coef < 0 {
			 scalar dummy = cond(runiform()<`select',1,.)
			 scalar coef = dummy*coef
			 scalar secoef = dummy*secoef
			 scalar tcoef = dummy*tcoef
		
		
		
		matrix A[`i',1] = coef
		matrix B[`i',1] = secoef
		matrix C[`i',1] = tcoef
		}

		
		matrix bob = A,B,C	
		svmat bob
		rename bob1 effect 
		rename bob2 seeffect 
		rename bob3 teffect 
		generate pet = (1/seeffect)
		
		metareg effect , wsse(seeffect) mm
		return scalar I2 = e(I2)
		
		summ effect, detail
		return scalar effectMED = r(p50)
		return scalar effectMIN = r(min)		
		return scalar effectP5 = r(p5)
		return scalar effectP95 = r(p95)
		return scalar effectMAX = r(max)
		return scalar N = r(N)
		
		summ teffect, detail
		return scalar teffectMED = r(p50)
		return scalar teffectMIN = r(min)		
		return scalar teffectP5 = r(p5)
		return scalar teffectP95 = r(p95)
		return scalar teffectMAX = r(max)
		
		gen absteffect=abs(teffect)
		gen sig=(absteffect>=2)
		summ sig
		return scalar pctsig = r(mean)

end
