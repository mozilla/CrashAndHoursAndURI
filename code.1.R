CS <- rhoptions()$tem$colsummer
E <- expression({
    suppressPackageStartupMessages(library(data.table))
    suppressPackageStartupMessages(library(Hmisc))
    suppressPackageStartupMessages(library(rjson))
})
isn <- function(x,r=NA) if(is.null(x) || length(x)==0) r else x


z <- rhwatch(map=expression({
    tryCatch({
        z <- fread(paste(unlist(map.values),collapse="\n")
                  , colClasses=c('character','character','character','character',
                                rep('integer',7)))
        setnames(z,c("cid","fxver","osver","date","contentcr","mediacr","plugincr","browsercr","hrs","uri","domain"))
        z[, date:=as.Date(date,"%Y%m%d")]
        z[is.na(contentcr), contentcr := 0]
        z[is.na(mediacr), mediacr := 0]
        z[is.na(plugincr), plugincr := 0]
        z[is.na(browsercr), browsercr := 0]
        z[is.na(uri), uri := 0]
        z[, allplugincr:=mediacr+plugincr]
        z[is.na(domain), domain := 0]
        rhcollect(sample(1:1000,1), z)
    }, error=function(e) { rhcounter("errors",as.character(e),1)})
   }), reduce=0, input=rhfmt("text",folders="s3://mozilla-metrics/sguha/uricrashes")
   ,output="/x",read=FALSE,setup=E)

z <- rhwatch(map=function(a,b){
    b[, rhcollect(.BY$cid, .SD),by=cid]
}, reduce=expression(
       pre = {
           .r <- NULL
       },
       reduce = {
           .r <- rbind(.r,rbindlist(reduce.values))
       },
       post = {
           .r <- .r[order(date),]
           rhcollect(reduce.key, .r)
       }
   ), mapred=list(mapred.reduce.tasks=300),
input='/x', output='s3://mozilla-metrics/sguha/tmp/urihrs',setup=E,read=FALSE)


I <- 's3://mozilla-metrics/sguha/tmp/urihrs'
t <- rh(I,E)


p <- t(map=function(a,b){
    if(nrow(b)>10) rhcollect(list(x=1,type=b[, round(cor(hrs, uri,method='kendall'),4)]),1)
})
pd <- rsp(p$collect(),c("d","x","n"))



p1 <- t(map=function(a,b){
    if(nrow(b)>10) rhcollect(list(x=1,type=b[hrs>0 & uri>0, round(cor((contentcr+allplugincr)/hrs, (contentcr+allplugincr)/uri*1000,method='kendall'),4)]),1)
})
p1d <- rsp(p1$collect(),c("d","x","n"))



p1 <- t(map=function(a,b){
    if(nrow(b)>5)
        rhcollect(list(x=1,type=b[hrs>0 & uri>0, round(isn(cor((contentcr+allplugincr)/hrs, (contentcr+allplugincr)/uri*1000,method='kendall'),-2),4)]),1)
})
p1d <- rsp(p1$collect(),c("d","x","n"))

p2 <- t(map=function(a,b){ rhcollect(list(x=1),c(1, 1*(nrow(b)>5)))})
p2d <- rsp(p2$collect(),c("x","n1","n2"))


p3 <- t(map=function(a,b){
    if(nrow(b)>5){
        co <- b[,cor((contentcr+allplugincr)/hrs, (contentcr+allplugincr)/uri*1000,method='kendall')]
        tryCatch(if(isn(co,-1)>0.95) rhcollect(a,b),error=function(e) NULL)
    }}, reduce=0)
                              

p4 <- t(map=function(a,b){
    if(nrow(b)>5){
        co <- b[hrs>0 & uri>0,list(h=mean( hrs,na.rm=TRUE), mean(uri,na.rm=TRUE))]
        rhcollect(sample(1:100,1), co)
    }}, reduce=rr)
                              
p4d = rbindlist(lapply(p4$collect(), "[[",2))
p4d[,cor(h,V2,use='pairwise.complete.obs')]


p6 <- t(map=function(a,b){
    if(nrow(b)>5){
        x <- runif(1)
        co <- b[hrs>0 & uri>0,][,list(cid=rep(x,.N),c=contentcr+allplugincr,u= uri,h=hrs)]
        rhcollect(sample(1:100,1), co)
    }}, reduce=dtbinder)
                              
p6d = rbindlist(lapply(p6$collect(), "[[",2))

p6d2 <- p6d[,list(
    o=cor(h,u,use='pairwise.complete.obs'),
    cr=cor(c/h,c/u,use='pairwise.complete.obs')
    ),by=cid]

p6d3 <- p6d[,if(.N>2 && any(c>0)) list(cr=cor(c/(h+1),c/(u+1),use='pairwise.complete.obs',method='kendall')),by=cid]
p6d4 <- p6d[,if(.N>2) list(cr=cor((c+1)/(h+1),(1+c)/(u+1),use='pairwise.complete.obs',method='kendall')),by=cid]



## Seee my moleskin for why this is best
ff=p6d[, list(x0=1,x1=if(.N>2) 1 else 0, x3=if(.N>2 && all(c==0)) 1 else 0,x2=if(.N>2 && any(c>0)) 1 else 0), by=cid]

p6d3 <- p6d[,if(.N>2 && any(c>0)) list(cr=cor(c/(h+1),c/(u+1),use='pairwise.complete.obs',method='kendall')),by=cid]


p6d5 <- p6d[,.SD[c>0,], by=cid][,if(.N>2) list(cr=cor(c/(h+1),c/(u+1),use='pairwise.complete.obs',method='kendall')),by=cid]



p6d4 <- p6d[,if(.N>2) list(ch=sum(c)/(sum(h)+1), cu=sum(c)/(sum(u)+1)), by=cid]
p6d4[, cor(ch,cu, method='kendall')]
p6d4[ sample( 1:nrow(p6d4), 0.05*nrow(p6d4)),][, cor(ch,cu, method='kendall')]


## Date Ranges
p7 <- t(map=function(a,b){ if(nrow(b)>1) b[, rhcollect( list(x=1, r = as.integer(diff(range(date)))),1) ]})
p7d=rsp(p7$collect(), c("x","r","n"))
p7d[, wtd.quantile(r,n,0:10/10)]


p8 <- t(map=function(a,b){ if(nrow(b)>1) b[, rhcollect( list(x=1, r = .N),1) ]})
p8d=rsp(p8$collect(), c("x","r","n"))
p8d[, wtd.quantile(r,n,0:10/10)]




p9d=t(map=function(a,b) b[, rhcollect(list(date=.BY$date),c(1,h=sum(hrs,na.rm=TRUE),u=sum(uri), crp = 1*(sum( c(contentcr,allplugincr)) >0),
                                                  cr=sum(c(contentcr,allplugincr)))),by=date] )$collect()
p9d= rsp(p9d, c("date","n","h","u","crp","cr"))
p9d$date <- as.Date(p9d$date,origin="1970-01-01")
p9d <- p9d[order(date),]
p9d[, ":="( m1 = cr/h, m2= cr/u,p=crp/n)]
p9d[, ":="(m1s = scale(m1),  m1n = (m1 - min(m1))/(max(m1)-min(m1)), m2s = scale(m2),m2n=(m2 - min(m2))/(max(m2)-min(m2)))]

pdf("j.pdf",width=10)
xyplot( m1s + m2s ~ as.Date(date), auto.key=TRUE, lwd=2,type=c("l",'g'),scale=list(tick.num=20),data=p9d)
xyplot( m1n + m2n ~ as.Date(date), auto.key=TRUE, lwd=2,type=c("l",'g'),scale=list(tick.num=20),data=p9d)
xyplot( m2s ~ m1s , auto.key=TRUE, lwd=2,type=c("l",'g'),scale=list(tick.num=20),data=p9d[order(m1s),])
xyplot( m2n ~ m1n , auto.key=TRUE, lwd=2,type=c("l",'g'),scale=list(tick.num=20),data=p9d[order(m1n),])
xyplot( m2s ~ m1s , auto.key=TRUE, lwd=2,type=c("p",'g','smooth'),scale=list(tick.num=20),data=p9d[order(m1s),][m1n<0.4,])
xyplot( m2n ~ m1n , auto.key=TRUE, lwd=2,type=c("p",'g','smooth'),scale=list(tick.num=20),data=p9d[order(m1n),][m1n<0.4,])
dev.off()
