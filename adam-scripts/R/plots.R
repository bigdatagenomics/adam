#!/usr/bin/Rscript

# Copyright 2014 Genome Bridge LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.                                          

mapqraw <- read.table("mapqs", sep="\t", header=TRUE)
baseqraw <- read.table("baseqs", sep="\t", header=TRUE)
dupes <- read.table("dupemismatch", sep="\t", header=TRUE)
positions <- read.table("positions", sep="\t", header=TRUE)
files <- read.table("files")

flattentable <- function(table) {
    i <- 1
    mod <- lapply(parse(text=paste("c",table[[1]])), eval)
    ret <- data.frame()
    while(i <= length(mod)) {
        j <- 1
        t <- mod[[i]]
        while(j <= length(t)) {
            ret[i, j] <- t[j]
            j <- j + 1
        }
        ret[i, j] <- table[i, 2]
        i <- i + 1
    }
    ret
}

mapqs <- flattentable(mapqraw)
baseqs <- flattentable(baseqraw)

upto <- function(high, values) {
    position <- Position(Negate(function (x) { x <= high }), values)
    if (is.na(position)) return(length(values))
    position-1
}

splitby <- function(splits, values, counts) {
    returns <- c()
    names <- c()
    i <- 1
    startat <- 1

    while (i <= length(splits)) {
        currentsplit <- splits[i]
        endat <- upto(currentsplit, values)

        returns[i] <- sum(counts[startat:endat])
        
        startat <- endat + 1
        i <- i + 1
    }

    names[1] <- paste("<", splits[1] + 1)
    i <- 2
    while (i <= length(splits)) {
        if (splits[i-1] + 1 == splits[i]) {
            names[i] <- paste(splits[i])
        } else {
            names[i] <- paste(splits[i-1] + 1, "-", splits[i])
        }
        i <- i + 1
    }
    names[i] <- paste(">", splits[i-1])

    returns[i] <- sum(counts[startat:length(values)])
    list(returns, names)
}

sorted.positions <- positions[order(positions[[2]]), ]

splits <- c(-1, 0, 20, 75)

old.par <- par(mfrow=c(2,2))
symbols(x=mapqs[[1]],
        y=mapqs[[2]],
        circles=log(mapqs[[3]]),
        inches=1/3,
        ann=F,
        bg=rgb(0, 0, 1, 0.1),
        fg=NULL)
title(main="Map Scores",
      xlab=files[1, 1],
      ylab=files[2, 1],
      sub="Size is log(count)")

symbols(x=baseqs[[1]],
        y=baseqs[[2]],
        circles=log(baseqs[[3]]),
        inches=1/3,
        ann=F,
        bg=rgb(0, 0, 1, 0.1),
        fg=NULL)
title(main="Base Scores",
      xlab=files[1, 1],
      ylab=files[2, 1],
      sub="Size is log(count)")

allsplits <- splitby(splits, sorted.positions$Value, sorted.positions$Count)
text(barplot(allsplits[[1]], names.arg=allsplits[[2]]), 0, allsplits[[1]], cex=1, pos=3)
title(main="Positions")

dev.copy(pdf, 'plots.pdf')
par(old.par)
dev.off()
