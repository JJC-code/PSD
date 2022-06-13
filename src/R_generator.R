mean_test = c(0.004, 0.002, 0.002, 0.004, 0.003, 0.001)
cov = matrix(c(1,1,0,1,1,1, 
             1,36,-1,-1,-3,-5, 
             0,-1,4,2,2,0,
             1,-1,2,49,-5,-2,
             1,-3,2,-5,16,-2,
             1,-5,0,-2,-2,9), 6, 6)
df=4
lower = c(-0.1,-0.1,-0.1,-0.1,-0.1,-0.1) 
upper = c(0.1, 0.1, 0.1, 0.1, 0.1, 0.1)


yields <- tmvtnorm::rtmvt(n=1000000, mean=mean_test, sigma=cov, df=df, lower=lower, upper=upper, algorithm = "gibbs")

write.table(yields, file='yields.txt', append = FALSE, sep = " ", dec = ".",
            row.names = TRUE, col.names = FALSE)
