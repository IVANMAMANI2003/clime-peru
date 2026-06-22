const { Document, Packer } = require('docx');
const fs = require('node:fs');
const path = require('node:path');

const p1 = require('./climeperu_doc_p1.js');
const p2 = require('./climeperu_doc_p2.js');

async function generateCompleteDocx() {
  console.log('Generando documento .docx — ClimePeru Unidad 2...');

  try {
    const allContent = [
      ...p1.coverPage(),
      ...p1.chapter1(),
      ...p1.chapter2(),
      ...p1.chapter3(),
      ...p2.chapter4(),
      ...p2.chapter5(),
      ...p2.chapter6(),
      ...p2.chapter7(),
      ...p2.chapter8(),
      ...p2.chapter9(),
      ...p2.chapter10(),
      ...p2.chapter11(),
    ];

    const validContent = allContent.filter(item => {
      if (!item) return false;
      return item.constructor && (
        item.constructor.name === 'Paragraph' ||
        item.constructor.name === 'Table'
      );
    });

    console.log(`  Elementos totales: ${allContent.length}`);
    console.log(`  Elementos válidos: ${validContent.length}`);

    const numberingConfig = {
      config: [
        {
          reference: "bullets",
          levels: [
            { level: 0, format: "bullet", text: "\u2022", alignment: "left" },
            { level: 1, format: "bullet", text: "\u25CB", alignment: "left" },
            { level: 2, format: "bullet", text: "\u25A0", alignment: "left" },
          ],
        },
        {
          reference: "numbers",
          levels: [
            { level: 0, format: "decimal", text: "%1.", alignment: "left" },
            { level: 1, format: "lowerLetter", text: "%2)", alignment: "left" },
          ],
        },
      ],
    };

    const doc = new Document({
      numbering: numberingConfig,
      sections: [{
        children: validContent
      }]
    });

    const buffer = await Packer.toBuffer(doc);
    const outputPath = path.join(__dirname, 'ClimePeru_Unidad2_Documentacion_Completa.docx');

    fs.writeFileSync(outputPath, buffer);
    console.log(`\u2713 Documento generado: ${outputPath}`);
    console.log(`  Tamaño: ${(buffer.length / 1024).toFixed(1)} KB`);
    console.log(`  Bytes: ${buffer.length}`);

  } catch (err) {
    console.error('Error:', err.message);
    console.error(err.stack);
  }
}

generateCompleteDocx();
