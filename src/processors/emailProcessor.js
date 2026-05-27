// A processor is just an async function that receives a job
// and does the actual work. Think of it like your DSA
// problem's "solve" function — given input (job.data),
// produce output or throw an error.

async function processEmailJob(job) {
    const { to, subject } = job.data;

    if (!to || !subject) throw new Error('Missing to or subject');
  
    // Update progress (visible in dashboard later)
    await job.updateProgress(10);
  
    console.log(`[Email] Sending to ${to}`);
  
    // Simulate email sending (takes 2 seconds)
    await new Promise(r => setTimeout(r, 200));
    await job.updateProgress(60);

    await new Promise(r => setTimeout(r, 100));
    await job.updateProgress(100);
  
    // Whatever you return is stored as the job's result
    return { sent:true, to, sentAt:new Date().toISOString() };
  }
  
  module.exports = { processEmailJob };