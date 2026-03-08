import { PDFDocument, rgb, degrees } from 'pdf-lib';
import fontkit from '@pdf-lib/fontkit';
import fs from 'fs/promises';

const in2pt = (inches) => inches * 72; 

export async function generateFoldableS12Card(territory, mapImageBuffer, doNotCalls = []) {
    try {
        const mainDoc = await PDFDocument.create();
        mainDoc.registerFontkit(fontkit);
        const fontBytes = await fs.readFile('./assets/fonts/ibm-plex-sans-latin-500-normal.woff2');
        const ibmFont = await mainDoc.embedFont(fontBytes);
        const page = mainDoc.addPage([in2pt(8.5), in2pt(11)]);

        page.drawRectangle({
            x: in2pt(1.5), y: in2pt(2.0), width: in2pt(5.5), height: in2pt(7.0),
            borderColor: rgb(0.8, 0.8, 0.8), borderWidth: 1,
        });

        const templateBytes = await fs.readFile('./assets/print/S-12alternate-E.pdf');
        const templateDoc = await PDFDocument.load(templateBytes);
        const [s12Front] = await mainDoc.embedPages(templateDoc.getPages());

        page.drawPage(s12Front, { x: in2pt(1.5), y: in2pt(5.5), width: in2pt(5.5), height: in2pt(3.5) });
        page.drawText(territory.locality || '', { x: in2pt(3.0), y: in2pt(8.2), size: 10, font: ibmFont, color: rgb(0,0,0) });
        page.drawText(territory.territory_no || '', { x: in2pt(5.8), y: in2pt(8.2), size: 10, font: ibmFont });

        const backDoc = await PDFDocument.create();
        backDoc.registerFontkit(fontkit);
        const backIbmFont = await backDoc.embedFont(fontBytes);
        const backPage = backDoc.addPage([in2pt(5.5), in2pt(3.5)]);

        const mapImage = await backDoc.embedPng(mapImageBuffer); 
        backPage.drawImage(mapImage, { x: 0, y: 0, width: in2pt(5.5), height: in2pt(3.5) });

        backPage.drawRectangle({
            x: in2pt(0.15), y: in2pt(0.15), width: in2pt(2.2), height: in2pt(3.2),
            color: rgb(1, 1, 1), opacity: 0.92,
        });

        backPage.drawText(`Territory: ${territory.territory_no}`, { x: in2pt(0.25), y: in2pt(3.0), size: 12, font: backIbmFont });
        backPage.drawText(`Locality: ${territory.locality}`, { x: in2pt(0.25), y: in2pt(2.75), size: 9, font: backIbmFont });

        if (doNotCalls.length > 0) {
            backPage.drawText('DO NOT CALLS:', { x: in2pt(0.25), y: in2pt(2.4), size: 8, font: backIbmFont, color: rgb(0.8, 0, 0) });
            let dncY = in2pt(2.2);
            doNotCalls.forEach((dnc, index) => {
                if (index > 10) return;
                backPage.drawText(`• ${dnc.address_full} ${dnc.apt ? `Apt ${dnc.apt}` : ''}`, { x: in2pt(0.25), y: dncY, size: 7, font: backIbmFont });
                dncY -= 10;
            });
        } else {
            backPage.drawText('No Do Not Calls recorded.', { x: in2pt(0.25), y: in2pt(2.4), size: 7, font: backIbmFont, color: rgb(0.3, 0.3, 0.3) });
        }

        const [embeddedBack] = await mainDoc.embedPages(backDoc.getPages());
        page.drawPage(embeddedBack, {
            x: in2pt(7.0), y: in2pt(5.5), width: in2pt(5.5), height: in2pt(3.5), rotate: degrees(180)
        });

        const pdfBytes = await mainDoc.save();
        await fs.writeFile(`./exports/S-12_Foldable_${territory.territory_no}.pdf`, pdfBytes);
    } catch (error) {
        console.error("Failed to generate foldable S-12 PDF:", error);
    }
}