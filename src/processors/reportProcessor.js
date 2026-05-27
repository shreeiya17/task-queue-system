async function processReportJob(job) {
    const { userId, reportType } = job.data;
    if (!userId) throw new Error('Missing userId');

    await job.updateProgress(20);
    await new Promise(r => setTimeout(r, 500));  // data fetch
    await job.updateProgress(60);
    await new Promise(r => setTimeout(r, 800));  // PDF generation
    await job.updateProgress(100);

    return {
        generated: true, userId, reportType,
        url: `https://storage.example.com/${userId}/${Date.now()}.pdf`,
        generatedAt: new Date().toISOString(),
    };
}
module.exports = { processReportJob };