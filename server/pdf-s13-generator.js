import { PDFDocument, rgb } from 'pdf-lib';
import fontkit from '@pdf-lib/fontkit';
import fs from 'fs/promises';

const in2pt = (inches) => inches * 72;

export async function generateS13MasterRecord(serviceYear, territoriesWithHistory) {
    try {
        const templateBytes = await fs.readFile('./assets/print/S-13_E.pdf');
        const pdfDoc = await PDFDocument.load(templateBytes);
        pdfDoc.registerFontkit(fontkit);
        const fontBytes = await fs.readFile('./assets/fonts/ibm-plex-sans-latin-500-normal.woff2');
        const ibmFont = await pdfDoc.embedFont(fontBytes);
        
        const page = pdfDoc.getPages()[0];
        page.drawText(serviceYear || new Date().getFullYear().toString(), { x: in2pt(7.0), y: in2pt(10.2), size: 12, font: ibmFont, color: rgb(0,0,0) });

        let startY = in2pt(9.2); 
        const rowHeight = 18; 
        const colTerrNo = in2pt(0.6);
        const colLastCompleted = in2pt(1.2);
        
        const assignmentBlocks = [
            { name: in2pt(2.0), out: in2pt(3.2), in: in2pt(3.8) },
            { name: in2pt(4.4), out: in2pt(5.6), in: in2pt(6.2) },
            { name: in2pt(6.8), out: in2pt(8.0), in: in2pt(8.6) }
        ];

        territoriesWithHistory.forEach((territory, rowIndex) => {
            if (rowIndex > 30) return; 
            const currentY = startY - (rowIndex * rowHeight);
            page.drawText(territory.territory_no || '', { x: colTerrNo, y: currentY, size: 9, font: ibmFont });
            
            if (territory.last_completed_date) {
                page.drawText(territory.last_completed_date, { x: colLastCompleted, y: currentY, size: 8, font: ibmFont });
            }

            const history = territory.assignment_history || [];
            history.forEach((record, blockIndex) => {
                if (blockIndex >= assignmentBlocks.length) return; 
                const block = assignmentBlocks[blockIndex];
                let conductorName = record.conductor_name || '';
                if (conductorName.length > 15) conductorName = conductorName.substring(0, 15) + '.';

                page.drawText(conductorName, { x: block.name, y: currentY, size: 8, font: ibmFont });
                page.drawText(record.date_assigned || '', { x: block.out, y: currentY, size: 8, font: ibmFont });
                page.drawText(record.date_completed || '', { x: block.in, y: currentY, size: 8, font: ibmFont });
            });
        });

        const pdfBytes = await pdfDoc.save();
        return pdfBytes; 
    } catch (error) {
        throw error;
    }
}