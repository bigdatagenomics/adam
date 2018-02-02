from pylab import loglog, ylabel, xlabel, title, grid, savefig, show, legend, xticks, yticks, figure, xlim, ylim

def setup(n, st):
    exp_n = [1.0 / st, (n [-1] / n [0]) / st]
    l_n = [n [0], n [-1]]

    figure ()
    loglog (l_n, exp_n, 'k-', basex=2, basey=2, label="Ideal Speedup")

def plot (n, mt, label_name, pattern):

    speedup = []
    
    for m in mt:
        
        speedup.append (1.0 / m)

    loglog (n, speedup, pattern, basex=2, basey=2, label=label_name)

def label(name, t, lloc=2):
    locs,labels = xticks()
    xn = ["", "32", "64", "128", "256", "512", "1024", ""]
    xticks(locs, xn)

    yn = ["", "32K", "16K", "8K", "4K", "2K", "1K", "500", ""]
    locs,labels = yticks()
    yticks(locs, yn)

    ylabel ("Runtime (seconds)")
    xlabel ("Number of Threads")
    legend (loc=lloc)
    title (t)
    grid (True)
    savefig (name)

n_ideal = [32, 1024]

n = [32, 128, 256, 512, 1024]
markdup = [16639.22, 4438.37, 2005.25, 1247.36, 844.03]
frag_md = [8249.56, 2594.44, 1409.86, 868.19, 529.19]
gatk_md = [17068.58, 4036.25, 1737.97, 991.62, 589.37]
bqsr = [27034.11, 7461.35, 4663.84, 2977.69, 2108.43]
gatk_bqsr = [(28232.96 + 2931.97), (8473.64 + 1312.90), (5578.24 + 732.01), (3465.61 + 551.55), (2410.03 + 487.16)]
ir = [23808.67, 6476.63, 3507.99, 2407.57, 1242.10]

setup(n_ideal, frag_md[0])

plot(n, markdup, 'ADAM Mark Duplicates', 'bx-')
plot(n, frag_md, 'ADAM Fragments Mark Duplicates', 'bo--')
plot(n, gatk_md, 'GATK4 Mark Duplicates', 'c.--')

label("speedup-md.pdf",
      "Duplicate Marking Speedup on NA12878 (High Coverage)", lloc=4)

setup(n_ideal, bqsr[0])

plot(n, bqsr, 'ADAM BQSR', 'bx-')
plot(n, gatk_bqsr, 'GATK4 BQSR', 'c.--')

label("speedup-bqsr.pdf",
      "Base Recalibration Speedup on NA12878 (High Coverage)")

setup(n_ideal, ir[0])

plot(n, ir, 'INDEL Realignment', 'bx-')

label("speedup-ir.pdf",
      "INDEL Realignment Speedup on NA12878 (High Coverage)")
