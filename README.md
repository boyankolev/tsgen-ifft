# Frequency-based time series generator

This tool generates random time series with desired frequency characteristics. This is done by first generating the frequency spectra of the time series and then applying an inverse Fourier transform plus some additional white noise to generate the series themselves. The spectrum generation assigns high amplitudes to `freqnum` subsequent frequency components, starting  from  the `freqstart`<sup>th</sup> one,  and  low  ones  for the rest. Randomness is applied to the phase shifts of all frequencies, so that the generated set of series can enjoy a good variety.

## Usage
<pre>

$SPARK_HOME/bin/spark-submit tsgen_ifft.py [-h] -fn FREQNUM [-fs FREQSTART] -tl TSLEN -tn TSNUM -o OUTFILE

Arguments:
  -h, --help            show this help message and exit
  -fn FREQNUM           number of significant frequencies
  -fs FREQSTART         first frequency index [default=0]
  -tl TSLEN             length of time series
  -tn TSNUM             number of time series
  -o OUTFILE            output file
</pre>

### Prerequisites

- Python, version 2.7 or later
- numpy, version 1.16 or later
- Spark, version 2.2.0 or later

