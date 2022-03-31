//  Author:  Rylan Marianchuk
//  March 2022
//

#include <iostream>
#include <thrust/sort.h>
#include <cstdlib>


/*
    Return the curve length of the signal

    float * signal:     starting memory address of signal to compute
    size_t SIGNAL_SIZE: number of samples of the signal
*/
__device__ float curvelength(float * signal, size_t SIGNAL_SIZE){
    float CL = 0;
    for (int j = 0; j < SIGNAL_SIZE - 1; j++) {
        float x1 = signal[j];
        float x2 = signal[j+1];
        CL += std::sqrt(1.0f + (x2 - x1)*(x2 - x1));
    }
    return CL;
}


/*
    Return the entropy of the normalized histogram of the signal

    float * signal:     starting memory address of signal to compute
    size_t SIGNAL_SIZE: number of samples of the signal
    int bins:           bin amount of the histogram
*/
__device__ float histogram_entropy(float * signal, size_t SIGNAL_SIZE, int bins=40){

    thrust::sort(thrust::seq, signal, signal + SIGNAL_SIZE);
    float min = signal[0];
    float max = signal[SIGNAL_SIZE - 1];
    float sum = 0;

    float binSize = (max - min) / bins;
    float binCount = 0;
    for (size_t i = 0; i < SIGNAL_SIZE; i++){
        if (signal[i] <= min + binSize){
            binCount++;
        }
        else {
            double v = binCount / binSize / SIGNAL_SIZE;
            sum += std::log2(v) * (v);
            binCount = 1;
            min += binSize;
        }
    }
    float v = binCount / binSize / SIGNAL_SIZE;
    sum += std::log2(v) * (v);

    return -sum;
}


/*
    Return yes if the signal does not change amplitude for 20 samples

    float * signal:     starting memory address of signal to compute
    size_t SIGNAL_SIZE: number of samples of the signal
*/
__device__ int has_flat20_samples(float * signal, size_t SIGNAL_SIZE){
    float prev = signal[0];
    int longest = 0;
    for (size_t i = 1; i < SIGNAL_SIZE; i++){
        if (signal[i] == prev){
            longest++;
        }
        else{
            longest = 0;
        }

        prev = signal[i];

        if (longest == 20)
            return 1;
    }
    return 0;
}


/*
    CUDA Kernel - analogous parameters as below, but pointers are on device
    const size_t SIGNAL_SIZE:   the boundary of signals within the container
*/
__global__ void GpuCompute(float * d_ecg_container,
                           float * d_resCL,
                           float * d_resHE,
                           float * d_resAC,
                           int * d_res20flat,
                           const size_t SIGNALS,
                           const size_t SIGNAL_SIZE){

    int i = blockIdx.x * blockDim.x + threadIdx.x;
    int stride = blockDim.x * gridDim.x;

    while (i < SIGNALS){
        float * signal = d_ecg_container + (i * SIGNAL_SIZE);

        // Curve Length
        d_resCL[i] = curvelength(signal, SIGNAL_SIZE);

        // Autocorrelation similarity
        //d_resAC[i] = segment_autocorr_sim(signal, SIGNAL_SIZE);

        // 20 values equal in sequence?
        d_res20flat[i] = has_flat20_samples(signal, SIGNAL_SIZE);

        // Histogram Entropy - do this last because of inplace sort
        d_resHE[i] = histogram_entropy(signal, SIGNAL_SIZE);
        i += stride;
    }

}


/*
    To invoke from python
    Input:
    float * ecg_container:    memory of the signals to compute features of

    To populate:
    float * resCL:            resCL[i] is the Curve Length of the ith signal given
    float * resHE:            resHE[i] is the Histogram Entropy of the ith signal given
    float * resAC:            not implemented
    int * res20flat:          binary vector, if ith value is 1, ith signal has some segment with no amplitude change for 20 samples

    const size_t SIGNALS:       number of signals within the container
*/
extern "C" {
void GetWvfmFeaturesGPU(float * ecg_container,
                        float * resCL,
                        float * resHE,
                        float * resAC,
                        int * res20flat,
                        const size_t SIGNALS
){
    // Increase the limit of the heap on device
    cudaError_t err = cudaDeviceSetLimit(cudaLimitMallocHeapSize, 1048576ULL*1024);

    // Declaring device pointers and initializing their memory
    float *d_ecg_container, *d_resCL, *d_resHE, *d_resAC;
    int * d_res20flat;

    // Defining the boundaries of the signals
    const size_t SIGNAL_SIZE = 5000;

    // Allocate memory of result containers on the device
    cudaMalloc(&d_resCL, sizeof(float) * SIGNALS);
    cudaMalloc(&d_resHE, sizeof(float) * SIGNALS);
    cudaMalloc(&d_resAC, sizeof(float) * SIGNALS);
    cudaMalloc(&d_res20flat, sizeof(int) * SIGNALS);

    // Copy the input signals to device
    cudaMalloc(&d_ecg_container, sizeof(float) * SIGNALS * SIGNAL_SIZE);
    cudaMemcpy(d_ecg_container, ecg_container, sizeof(float) * SIGNALS * SIGNAL_SIZE, cudaMemcpyHostToDevice);


    // Invoke Kernel
    const unsigned tpb_x = 256;
    const unsigned bpg_x = (SIGNALS + tpb_x - 1) / tpb_x;
    dim3 blocksPerGrid(bpg_x, 1, 1);
    dim3 threadsPerBlock(tpb_x, 1, 1);
    GpuCompute<<<blocksPerGrid, threadsPerBlock>>>(d_ecg_container, d_resCL, d_resHE, d_resAC, d_res20flat, SIGNALS, SIGNAL_SIZE);

    // Transfer result containers back to device
    cudaMemcpy(resCL, d_resCL, sizeof(float)*SIGNALS, cudaMemcpyDeviceToHost);
    cudaMemcpy(resHE, d_resHE, sizeof(float)*SIGNALS, cudaMemcpyDeviceToHost);
    cudaMemcpy(resAC, d_resAC, sizeof(float)*SIGNALS, cudaMemcpyDeviceToHost);
    cudaMemcpy(res20flat, d_res20flat, sizeof(int)*SIGNALS, cudaMemcpyDeviceToHost);

    cudaFree(d_ecg_container);
    cudaFree(d_resCL);
    cudaFree(d_resHE);
    cudaFree(d_resAC);
    cudaFree(d_res20flat);
}
}
